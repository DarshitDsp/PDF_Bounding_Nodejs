#!/usr/bin/env python3
"""
Enhanced Textract PDF analyzer: raw text, key→value, tables with bboxes & confidences.
Usage:
  python analyze_with_textractor.py <pdf_path> <s3_bucket> [<s3_key_prefix>]

Ensure AWS credentials and region are set (e.g. via AWS_PROFILE or env).
"""
import sys
import os
import time
import json
import boto3


def upload_to_s3(pdf_path: str, bucket: str, key: str):
    s3 = boto3.client("s3")
    s3.upload_file(pdf_path, bucket, key)


def start_analysis(bucket: str, key: str) -> str:
    client = boto3.client("textract")
    resp = client.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
        FeatureTypes=["FORMS", "TABLES"]
    )
    return resp["JobId"]


def get_job_results(job_id: str):
    client = boto3.client("textract")
    while True:
        resp = client.get_document_analysis(JobId=job_id)
        status = resp.get("JobStatus")
        print(f"Textract job status: {status}")
        if status in ("SUCCEEDED", "FAILED"):
            break
        time.sleep(5)
    if status != "SUCCEEDED":
        raise RuntimeError("Textract analysis failed.")
    return resp["Blocks"]


def parse_blocks(blocks: list) -> dict:
    by_id = {b["Id"]: b for b in blocks}

    # 1. Raw text items
    raw = []
    for b in blocks:
        if b["BlockType"] in ("WORD", "LINE"):
            geom = b.get("Geometry", {}).get("BoundingBox", {})
            raw.append({
                "Text": b.get("Text", ""),
                "Type": b["BlockType"],
                "Page": b.get("Page"),
                "Confidence": b.get("Confidence"),
                "BoundingBox": geom
            })

    # 2. Key→Value pairs
    kvs = []
    for b in blocks:
        if b["BlockType"] == "KEY_VALUE_SET" and "KEY" in b.get("EntityTypes", []):
            # assemble key text
            key_text = []
            for rel in b.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    for cid in rel["Ids"]:
                        w = by_id[cid]
                        if w["BlockType"] == "WORD":
                            key_text.append(w.get("Text", ""))
            # find value block
            val_rel = next((r for r in b.get("Relationships", []) if r["Type"] == "VALUE"), None)
            if not val_rel:
                continue
            val_block = by_id[val_rel["Ids"][0]]
            # assemble value text
            val_text = []
            for rel in val_block.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    for cid in rel["Ids"]:
                        w = by_id[cid]
                        if w["BlockType"] == "WORD":
                            val_text.append(w.get("Text", ""))
            kvs.append({
                "Key": " ".join(key_text).strip(),
                "Value": " ".join(val_text).strip(),
                "KeyBox": b.get("Geometry", {}).get("BoundingBox", {}),
                "ValueBox": val_block.get("Geometry", {}).get("BoundingBox", {}),
                "Confidence": {"Key": b.get("Confidence"), "Value": val_block.get("Confidence")}  
            })

    # 3. Tables → Cells → Words
    tables = []
    for b in blocks:
        if b["BlockType"] == "TABLE":
            cells = []
            for rel in b.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    for cid in rel["Ids"]:
                        cell = by_id[cid]
                        if cell["BlockType"] == "CELL":
                            # gather cell text
                            txt = []
                            for r2 in cell.get("Relationships", []):
                                if r2["Type"] == "CHILD":
                                    for wid in r2["Ids"]:
                                        w = by_id[wid]
                                        if w["BlockType"] == "WORD":
                                            txt.append(w.get("Text", ""))
                            cells.append({
                                "Row": cell.get("RowIndex"),
                                "Column": cell.get("ColumnIndex"),
                                "Text": " ".join(txt).strip(),
                                "BoundingBox": cell.get("Geometry", {}).get("BoundingBox", {}),
                                "Confidence": cell.get("Confidence")
                            })
            tables.append({"Page": b.get("Page"), "Cells": cells})

    return {"RawText": raw, "KeyValues": kvs, "Tables": tables}


def main():
    if len(sys.argv) < 3:
        print("Usage: python analyze_with_textractor.py <pdf_path> <s3_bucket> [<s3_key_prefix>]")
        sys.exit(1)

    pdf_path = sys.argv[1]
    bucket   = sys.argv[2]
    prefix   = sys.argv[3] if len(sys.argv) > 3 else ""
    key      = os.path.join(prefix.rstrip('/'), os.path.basename(pdf_path)) if prefix else os.path.basename(pdf_path)

    print(f"Uploading '{pdf_path}' → s3://{bucket}/{key}")
    upload_to_s3(pdf_path, bucket, key)

    print("Starting Textract job...")
    job_id = start_analysis(bucket, key)

    blocks = get_job_results(job_id)
    print(f"Fetched {len(blocks)} blocks")

    result = parse_blocks(blocks)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
