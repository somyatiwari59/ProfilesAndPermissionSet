#!/usr/bin/env python3
import os
import sys
import json
import glob
import subprocess
import argparse
import itertools
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd
import requests

# ---------- Helpers ----------

def run(cmd, check=True):
    print(f"+ {cmd}")
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and res.returncode != 0:
        print(res.stdout)
        print(res.stderr, file=sys.stderr)
        raise RuntimeError(f"Command failed: {cmd}")
    return res

def ensure_dir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def find_meta_paths(root: Path):
    # Handle both source format and mdapi format
    # Source format: force-app/main/default/(profiles|permissionsets)/*.meta.xml
    profiles = list(root.glob("**/profiles/*.profile-meta.xml"))
    permsets = list(root.glob("**/permissionsets/*.permissionset-meta.xml"))

    # mdapi format fallback:
    if not profiles and not permsets:
        profiles = list(root.glob("**/profiles/*.profile"))
        permsets = list(root.glob("**/permissionsets/*.permissionset"))

    return profiles, permsets

def parse_profile_or_permset(xml_path: Path):
    ns = {}
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # What we capture:
    user_perms = {}              # {'ModifyAllData': True/False, ...}
    object_perms = {}            # {'Account': {'C':bool,'R':bool,'U':bool,'D':bool,'ViewAll':bool,'ModifyAll':bool}, ...}
    field_perms = {}             # {'Account.Industry': {'R':bool,'E':bool}, ...}
    apex_access = set()          # { 'SomeClass' }

    # User permissions
    for up in root.findall("./userPermissions", ns):
        name = (up.findtext("name", default="") or "").strip()
        enabled = (up.findtext("enabled", default="false") or "").strip().lower() == "true"
        if name:
            user_perms[name] = enabled

    # Object permissions
    for op in root.findall("./objectPermissions", ns):
        obj = (op.findtext("object", default="") or "").strip()
        if not obj:
            continue
        perms = {
            "C": (op.findtext("allowCreate", default="false").lower() == "true"),
            "R": (op.findtext("allowRead",   default="false").lower() == "true"),
            "U": (op.findtext("allowEdit",   default="false").lower() == "true"),
            "D": (op.findtext("allowDelete", default="false").lower() == "true"),
            "ViewAll": (op.findtext("viewAllRecords",  default="false").lower() == "true"),
            "ModifyAll": (op.findtext("modifyAllRecords", default="false").lower() == "true"),
        }
        object_perms[obj] = perms

    # Field permissions
    for fp in root.findall("./fieldPermissions", ns):
        field = (fp.findtext("field", default="") or "").strip()
        if not field:
            continue
        readable = (fp.findtext("readable", default="false").lower() == "true")
        editable = (fp.findtext("editable", default="false").lower() == "true")
        field_perms[field] = {"R": readable, "E": editable}

    # Apex class access (profiles use applicationVisibilities/apexClassAccesses; permsets use classAccesses)
    # Try both
    for ca in root.findall("./classAccesses", ns):
        clz = (ca.findtext("apexClass", default="") or "").strip()
        enabled = (ca.findtext("enabled", default="false").lower() == "true")
        if clz and enabled:
            apex_access.add(clz)

    for ca in root.findall("./apexClassAccesses", ns):
        clz = (ca.findtext("apexClass", default="") or "").strip()
        enabled = (ca.findtext("enabled", default="false").lower() == "true")
        if clz and enabled:
            apex_access.add(clz)

    return user_perms, object_perms, field_perms, apex_access

def dicts_union_keys(dict_list):
    keys = set()
    for d in dict_list:
        keys |= set(d.keys())
    return sorted(keys)

# ---------- SF Auth + Retrieve ----------

def sf_jwt_auth():
    alias = os.environ.get("SF_ALIAS", "permsexport")
    username = os.environ["SF_USERNAME"]
    client_id = os.environ["SF_CONSUMER_KEY"]
    instance_url = os.environ.get("SF_INSTANCE_URL", "https://login.salesforce.com")
    jwt_key_file = os.environ["SF_JWT_KEY_FILE"]  # path to server.key

    run(f'sf org login jwt --username "{username}" --jwt-key-file "{jwt_key_file}" --client-id "{client_id}" --instance-url "{instance_url}" --alias "{alias}" --json')
    return alias

def sf_generate_manifest():
    # Only Profiles & PermissionSets
    ensure_dir(Path("manifest/permsOnly.xml"))
    run('sf project generate manifest --name permsOnly.xml --metadata "Profile,PermissionSet"')

def sf_retrieve(alias):
    # Retrieve into source format in the repo
    run(f'sf project retrieve start --manifest "manifest/permsOnly.xml" --target-org "{alias}"')

# ---------- Summarize to Excel ----------

def summarize_to_excel(source_root: Path, out_xlsx: Path):
    profiles, permsets = find_meta_paths(source_root)

    entries = []
    user_perm_matrix = {}
    object_perm_matrix = {}
    field_perm_matrix = {}
    apex_matrix = {}

    # Build per artifact
    for p in profiles:
        label = p.stem.replace(".profile-meta", "")
        up, op, fp, apx = parse_profile_or_permset(p)
        entries.append(("Profile", label, up, op, fp, apx))

    for p in permsets:
        label = p.stem.replace(".permissionset-meta", "")
        up, op, fp, apx = parse_profile_or_permset(p)
        entries.append(("PermSet", label, up, op, fp, apx))

    # User Permissions sheet
    all_userperm_names = dicts_union_keys([e[2] for e in entries])
    for typ, name, up, op, fp, apx in entries:
        col = f"{typ}:{name}"
        for perm in all_userperm_names:
            user_perm_matrix.setdefault(perm, {})[col] = bool(up.get(perm, False))

    df_user = pd.DataFrame.from_dict(user_perm_matrix, orient="index").sort_index()

    # Object Permissions sheet (explode CRUD flags)
    # Rows: Object, Columns: (Artifact) → C,R,U,D,ViewAll,ModifyAll
    all_objects = dicts_union_keys([e[3] for e in entries])
    obj_rows = []
    for obj in all_objects:
        row = {"Object": obj}
        for typ, name, up, op, fp, apx in entries:
            col_base = f"{typ}:{name}"
            perms = op.get(obj, {})
            for flag in ["C","R","U","D","ViewAll","ModifyAll"]:
                row[f"{col_base}:{flag}"] = bool(perms.get(flag, False))
        obj_rows.append(row)
    df_obj = pd.DataFrame(obj_rows).sort_values("Object")

    # Field Permissions sheet
    all_fields = dicts_union_keys([e[4] for e in entries])
    fld_rows = []
    for fld in all_fields:
        row = {"Field": fld}
        for typ, name, up, op, fp, apx in entries:
            col_base = f"{typ}:{name}"
            perms = fp.get(fld, {})
            row[f"{col_base}:R"] = bool(perms.get("R", False))
            row[f"{col_base}:E"] = bool(perms.get("E", False))
        fld_rows.append(row)
    df_fld = pd.DataFrame(fld_rows).sort_values("Field")

    # Apex Class Access sheet
    all_classes = sorted(set(itertools.chain.from_iterable([e[5] for e in entries])))
    apex_rows = []
    for clz in all_classes:
        row = {"ApexClass": clz}
        for typ, name, up, op, fp, apx in entries:
            col = f"{typ}:{name}"
            row[col] = clz in e[5] if False else None  # placeholder, fix below
        apex_rows.append(row)
    # Fill correctly (second pass)
    apex_rows = []
    for clz in all_classes:
        row = {"ApexClass": clz}
        for typ, name, up, op, fp, apx in entries:
            col = f"{typ}:{name}"
            row[col] = (clz in apx)
        apex_rows.append(row)
    df_apx = pd.DataFrame(apex_rows).sort_values("ApexClass")

    ensure_dir(out_xlsx)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df_user.to_excel(writer, sheet_name="UserPermissions")
        df_obj.to_excel(writer, sheet_name="ObjectPermissions", index=False)
        df_fld.to_excel(writer, sheet_name="FieldPermissions", index=False)
        df_apx.to_excel(writer, sheet_name="ApexClassAccess", index=False)

    print(f"Wrote Excel: {out_xlsx}")

# ---------- OneDrive upload (Graph app-only) ----------

def graph_app_token(tenant_id, client_id, client_secret):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }
    r = requests.post(url, data=data, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]

def upload_to_onedrive(token, upn_or_user_id, dest_path, local_file):
    # Application permission to a specific user's OneDrive:
    # Requires Files.ReadWrite.All (Application) + admin consent
    # Endpoint: /users/{user-id or upn}/drive/root:/path:/content
    base = "https://graph.microsoft.com/v1.0"
    url = f"{base}/users/{upn_or_user_id}/drive/root:/{dest_path}:/content"
    with open(local_file, "rb") as f:
        r = requests.put(url, headers={"Authorization": f"Bearer {token}"}, data=f)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"OneDrive upload failed: {r.status_code} {r.text}")
    print("Uploaded to OneDrive:", r.json().get("webUrl", "<no-url>"))

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(description="Export SF Profiles/PermSets to Excel and upload to OneDrive.")
    ap.add_argument("--source-root", default=".", help="Root folder to scan for retrieved metadata (default: repo root)")
    ap.add_argument("--output", default="artifacts/permissions-summary.xlsx", help="Excel output path")
    ap.add_argument("--skip-auth", action="store_true", help="Skip SF auth (if already authed in the runner)")
    ap.add_argument("--skip-retrieve", action="store_true", help="Skip retrieve (use existing metadata in repo)")
    ap.add_argument("--onedrive-dest", required=False, help="Destination path in OneDrive (e.g., Reports/permissions-summary.xlsx)")
    args = ap.parse_args()

    # SF Auth + Retrieve
    alias = os.environ.get("SF_ALIAS", "permsexport")
    if not args.skip_auth:
        alias = sf_jwt_auth()
    if not args.skip_retrieve:
        sf_generate_manifest()
        sf_retrieve(alias)

    # Summarize
    summarize_to_excel(Path(args.source_root), Path(args.output))

    # Upload to OneDrive (optional)
    if args.onedrive_dest:
        tenant = os.environ["MS_TENANT_ID"]
        client_id = os.environ["MS_CLIENT_ID"]
        client_secret = os.environ["MS_CLIENT_SECRET"]
        user = os.environ["MS_ONEDRIVE_USER"]  # UPN or User ID whose OneDrive to write into

        token = graph_app_token(tenant, client_id, client_secret)
        upload_to_onedrive(token, user, args.onedrive_dest, args.output)

if __name__ == "__main__":
    main()
