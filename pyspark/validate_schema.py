import logging

logger = logging.getLogger(__name__)

EXPECTED_ROOT = {"id": "string", "type": "string", "created_at": "string",
                  "public": "boolean", "actor": "struct", "org": "struct",
                  "repo": "struct", "payload": "struct"}

EXPECTED_PAYLOAD = {"action": "string", "ref": "string", "ref_type": "string",
                     "push_id": "long", "pull_request": "struct",
                     "issue": "struct", "release": "struct", "forkee": "struct"}


def _check(struct, expected):
    actual = {f.name: f.dataType.typeName() for f in struct.fields}
    missing = {c for c in expected if c not in actual}
    mismatched = {c: (expected[c], actual[c]) for c in expected
                  if c in actual and actual[c] != expected[c]}
    return missing, mismatched


def validate_schema_columns(df):
    missing, mismatched = _check(df.schema, EXPECTED_ROOT )

    if "payload" in dict(df.dtypes) and df.schema["payload"].dataType.typeName() == "struct":
        p_missing, p_mismatched = _check(df.schema["payload"].dataType, EXPECTED_PAYLOAD)
        missing.update(p_missing)
        mismatched.update(p_mismatched)

    if missing or mismatched:
        msg = f"Schema validation failed: missing={missing}, type_mismatches={mismatched}"
        logger.error(msg)
        
        raise ValueError(f"Schema validation failed: missing={missing}, type_mismatches={mismatched}")

    logger.info("Schema validation passed")