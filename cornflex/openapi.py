"""Turn an OpenAPI 3 spec (e.g. Spring's /v3/api-docs) into readable text.

spec = api.get_json("/v3/api-docs")
print(describe_openapi(spec))                                  # every endpoint, grouped by tag
print(find_endpoints(spec, "person"))                          # search
print(describe_endpoint(spec, "/v1/persons-list-request", "post"))  # request + responses
print(describe_schema(spec, "PersonsListRequest"))             # one model from components
"""

from typing import Any

_METHODS = ("get", "post", "put", "patch", "delete", "head", "options", "trace")
_MAX_DEPTH = 8


def list_endpoints(spec: dict) -> list[dict]:
    """Every operation as {method, path, summary, operation_id, tags}."""
    endpoints = []
    for path, item in spec.get("paths", {}).items():
        for method in _METHODS:
            if method in item:
                op = item[method]
                endpoints.append(
                    {
                        "method": method.upper(),
                        "path": path,
                        "summary": _first_line(
                            op.get("summary") or op.get("description") or ""
                        ),
                        "operation_id": op.get("operationId", ""),
                        "tags": op.get("tags") or ["(untagged)"],
                    }
                )
    return endpoints


def describe_openapi(spec: dict) -> str:
    """Title, version, servers and all endpoints grouped by tag."""
    info = spec.get("info", {})
    lines = [
        f"{info.get('title', '?')}  version {info.get('version', '?')}  "
        f"(OpenAPI {spec.get('openapi', '?')})"
    ]
    for server in spec.get("servers", []):
        lines.append(f"  server: {server.get('url')}")

    by_tag: dict[str, list[dict]] = {}
    for ep in list_endpoints(spec):
        for tag in ep["tags"]:
            by_tag.setdefault(tag, []).append(ep)

    for tag in sorted(by_tag):
        lines.append(f"\n[{tag}]")
        for ep in by_tag[tag]:
            lines.append(f"  {ep['method']:<7} {ep['path']:<50} {ep['summary']}")

    schemas = spec.get("components", {}).get("schemas", {})
    lines.append(
        f"\n{len(schemas)} schemas in components: {', '.join(sorted(schemas))}"
    )
    return "\n".join(lines)


def find_endpoints(spec: dict, text: str) -> str:
    """Endpoints whose path, summary, description or operationId contains `text` (case-insensitive)."""
    needle = text.lower()
    lines = []
    for path, item in spec.get("paths", {}).items():
        for method in _METHODS:
            if method not in item:
                continue
            op = item[method]
            haystack = " ".join(
                [
                    path,
                    op.get("summary", ""),
                    op.get("description", ""),
                    op.get("operationId", ""),
                ]
            ).lower()
            if needle in haystack:
                summary = _first_line(op.get("summary") or op.get("description") or "")
                lines.append(f"{method.upper():<7} {path:<50} {summary}")
    return "\n".join(lines) or f"no endpoints match '{text}'"


def describe_endpoint(spec: dict, path: str, method: str = "get") -> str:
    """Parameters, request body and responses for one operation, with $refs resolved."""
    item = spec.get("paths", {}).get(path)
    if item is None:
        raise ValueError(
            f"No endpoint '{path}' in spec. Use find_endpoints(spec, text) to search."
        )
    op = item.get(method.lower())
    if op is None:
        available = ", ".join(m.upper() for m in _METHODS if m in item) or "none"
        raise ValueError(
            f"'{path}' has no {method.upper()} operation (available: {available})"
        )
    lines = [f"{method.upper()} {path}"]
    if op.get("summary"):
        lines.append(f"  {op['summary']}")
    if op.get("description") and op.get("description") != op.get("summary"):
        lines.append(f"  {_first_line(op['description'], 200)}")

    params = [
        _resolve(spec, p) for p in item.get("parameters", []) + op.get("parameters", [])
    ]
    if params:
        lines.append("\nParameters  (* = required)")
        for p in params:
            star = "*" if p.get("required") else ""
            label = _type_label(spec, p.get("schema", {}))
            lines.append(
                f"  {p['name'] + star:<30} in={p.get('in', '?'):<7} {label:<30} "
                f"{_first_line(p.get('description', ''))}"
            )

    if "requestBody" in op:
        body = _resolve(spec, op["requestBody"])
        required = "required" if body.get("required") else "optional"
        for content_type, media in body.get("content", {}).items():
            schema = media.get("schema", {})
            lines.append(
                f"\nRequest body  {content_type}, {required}  -> {_type_label(spec, schema)}"
                f"{_constraints(_merged(spec, schema))}"
            )
            lines += _schema_lines(spec, schema, indent=1, seen=_seen_for(schema))

    lines.append("\nResponses")
    for code, response in op.get("responses", {}).items():
        response = _resolve(spec, response)
        content = response.get("content", {})
        if not content:
            lines.append(f"  {code}  {_first_line(response.get('description', ''))}")
        for content_type, media in content.items():
            schema = media.get("schema", {})
            lines.append(
                f"  {code}  {content_type}  -> {_type_label(spec, schema)}  "
                f"{_first_line(response.get('description', ''))}"
            )
            lines += _schema_lines(spec, schema, indent=2, seen=_seen_for(schema))
    return "\n".join(lines)


def describe_schema(spec: dict, name: str) -> str:
    """One model from components/schemas as a field tree."""
    ref = f"#/components/schemas/{name}"
    if name not in spec.get("components", {}).get("schemas", {}):
        raise ValueError(
            f"No schema '{name}' in components. describe_openapi(spec) lists them."
        )
    lines = [name]
    description = _resolve(spec, {"$ref": ref}).get("description")
    if description:
        lines.append(f"  {_first_line(description, 200)}")
    lines += _schema_lines(spec, {"$ref": ref}, indent=1, seen=frozenset({ref}))
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def _first_line(text: str, width: int = 70) -> str:
    line = (text or "").strip().splitlines()[0] if (text or "").strip() else ""
    return line if len(line) <= width else line[: width - 1] + "…"


def _resolve(spec: dict, node: Any) -> Any:
    """Follow local $refs ('#/components/...') until reaching a real object."""
    seen = set()
    while isinstance(node, dict) and "$ref" in node:
        ref = node["$ref"]
        if ref in seen or not ref.startswith("#/"):
            return node  # cycle or external ref: give up, show as-is
        seen.add(ref)
        target: Any = spec
        try:
            for part in ref[2:].split("/"):
                target = target[part.replace("~1", "/").replace("~0", "~")]
        except (KeyError, TypeError):
            return node  # dangling ref: show as-is
        node = target
    return node


def _merged(spec: dict, schema: dict) -> dict:
    """Resolve $ref and flatten allOf (Spring uses it for inheritance) into one object schema."""
    s = _resolve(spec, schema)
    if not isinstance(s, dict) or "allOf" not in s:
        return s if isinstance(s, dict) else {}
    merged: dict = {"type": "object", "properties": {}, "required": []}
    for part in s["allOf"]:
        p = _merged(spec, part)
        merged["properties"].update(p.get("properties", {}))
        merged["required"] += p.get("required", [])
        for key in ("enum", "format", "items"):
            if key in p:
                merged[key] = p[key]
    merged["properties"].update(s.get("properties", {}))
    merged["required"] += s.get("required", [])
    if not merged["properties"]:
        merged.pop("properties")
        if "items" in merged:
            merged["type"] = "array"
    return merged


def _type_label(spec: dict, schema: dict) -> str:
    """Short type description: 'string(date)', 'array<Person>', 'Status (string enum[A, B])'."""
    if not schema:
        return "any"
    if "$ref" in schema:
        name = schema["$ref"].rsplit("/", 1)[-1]
        target = _resolve(spec, schema)
        if "$ref" in target:
            return name  # unresolvable
        if target.get("type") in (None, "object") and (
            "properties" in target
            or "allOf" in target
            or target.get("type") == "object"
        ):
            return name
        return f"{name} ({_type_label(spec, target)})"

    if "allOf" in schema and len(schema["allOf"]) == 1:
        return _type_label(spec, schema["allOf"][0])
    for key in ("oneOf", "anyOf"):
        if key in schema:
            return (
                f"{key}(" + ", ".join(_type_label(spec, o) for o in schema[key]) + ")"
            )

    t = schema.get("type")
    if isinstance(t, list):  # OpenAPI 3.1: type: [string, "null"]
        nullable = "null" in t
        t = "|".join(x for x in t if x != "null")
    else:
        nullable = bool(schema.get("nullable"))

    if t == "array" or "items" in schema:
        label = f"array<{_type_label(spec, schema.get('items', {}))}>"
    elif (
        isinstance(schema.get("additionalProperties"), dict)
        and "properties" not in schema
    ):
        label = f"map<string, {_type_label(spec, schema['additionalProperties'])}>"
    else:
        if t is None and ("properties" in schema or "allOf" in schema):
            t = "object"
        label = t or "any"
        if schema.get("format"):
            label += f"({schema['format']})"
        if "enum" in schema:
            values = [str(v) for v in schema["enum"]]
            label += (
                " enum["
                + ", ".join(values[:6])
                + (", …" if len(values) > 6 else "")
                + "]"
            )
    if nullable:
        label += " | null"
    return label


def _inner_ref(schema: dict) -> str | None:
    """The $ref of a property, or of its array items."""
    if "$ref" in schema:
        return schema["$ref"]
    items = schema.get("items")
    if isinstance(items, dict) and "$ref" in items:
        return items["$ref"]
    return None


def _has_fields(spec: dict, schema: dict) -> bool:
    s = _merged(spec, schema)
    if s.get("type") == "array" or "items" in s:
        return _has_fields(spec, s.get("items", {}))
    return bool(s.get("properties")) or "oneOf" in s or "anyOf" in s


def _schema_lines(
    spec: dict, schema: dict, indent: int, seen: frozenset = frozenset()
) -> list[str]:
    """Field tree of a schema. '*' marks required fields. Nested objects are indented."""
    pad = "  " * indent
    if indent > _MAX_DEPTH:
        return [f"{pad}…"]
    s = _merged(spec, schema)

    if s.get("type") == "array" or "items" in s:
        return _schema_lines(spec, s.get("items", {}), indent, seen)

    for key in ("oneOf", "anyOf"):
        if key in s:
            lines = [f"{pad}{key}:"]
            for option in s[key]:
                lines.append(f"{pad}  - {_type_label(spec, option)}")
                ref = _inner_ref(option)
                if ref and ref in seen:
                    continue
                lines += _schema_lines(
                    spec, option, indent + 2, seen | ({ref} if ref else set())
                )
            return lines

    required = set(s.get("required", []))
    lines = []
    for name, prop in s.get("properties", {}).items():
        star = "*" if name in required else ""
        label = _type_label(spec, prop) + _constraints(_resolve(spec, prop))
        description = _first_line(_description(spec, prop), 60)
        lines.append(f"{pad}{name + star:<30} {label:<30} {description}".rstrip())

        ref = _inner_ref(prop)
        if ref and ref in seen:
            lines.append(f"{pad}  (recursive: {ref.rsplit('/', 1)[-1]}, shown above)")
        elif _has_fields(spec, prop):
            lines += _schema_lines(
                spec, prop, indent + 1, seen | ({ref} if ref else set())
            )
    return lines


def _seen_for(schema: dict) -> frozenset:
    ref = _inner_ref(schema)
    return frozenset({ref}) if ref else frozenset()


def _description(spec: dict, schema: dict) -> str:
    """Property description, falling back to the array items' description."""
    s = _resolve(spec, schema)
    if not isinstance(s, dict):
        return ""
    if s.get("description"):
        return s["description"]
    items = _resolve(spec, s.get("items", {}))
    return items.get("description", "") if isinstance(items, dict) else ""


def _constraints(schema: dict) -> str:
    """Limits worth knowing when building requests, e.g. ' [max 100 items]'."""
    if not isinstance(schema, dict):
        return ""
    parts = []
    for key, text in (
        ("minItems", "min {} items"),
        ("maxItems", "max {} items"),
        ("minLength", "min length {}"),
        ("maxLength", "max length {}"),
        ("minimum", ">= {}"),
        ("maximum", "<= {}"),
        ("pattern", "pattern {}"),
    ):
        if key in schema:
            parts.append(text.format(schema[key]))
    return f" [{', '.join(parts)}]" if parts else ""
