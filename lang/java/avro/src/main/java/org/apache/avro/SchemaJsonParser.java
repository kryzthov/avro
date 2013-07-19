package org.apache.avro;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.LockableArrayList;
import org.apache.avro.Schema.Type;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.ObjectNode;

public final class SchemaJsonParser {
  private static final JsonFactory JSON_FACTORY = new JsonFactory();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(JSON_FACTORY);

  private static final Set<String> RESERVED_FIELDS = new HashSet<String>();
  private static final Set<String> FIELD_RESERVED = new HashSet<String>();
  private static final Map<String, Type> PRIMITIVES = new HashMap<String, Type>();
  private static final Set<String> NAMED_TYPES = new HashSet<String>();

  static {
    PRIMITIVES.put("string",  Type.STRING);
    PRIMITIVES.put("bytes",   Type.BYTES);
    PRIMITIVES.put("int",     Type.INT);
    PRIMITIVES.put("long",    Type.LONG);
    PRIMITIVES.put("float",   Type.FLOAT);
    PRIMITIVES.put("double",  Type.DOUBLE);
    PRIMITIVES.put("boolean", Type.BOOLEAN);
    PRIMITIVES.put("null",    Type.NULL);

    Collections.addAll(NAMED_TYPES,
        "record", "enum", "error", "fixed");

    Collections.addAll(RESERVED_FIELDS,
        "doc", "fields", "items", "name", "namespace",
        "size", "symbols", "values", "type", "aliases");

    Collections.addAll(FIELD_RESERVED,
        "default","doc","name","order","type","aliases");
  }

  private final Map<String, JsonNode> mJsonMap = new HashMap<String, JsonNode>();
  private final Map<String, Schema> mSchemaMap = new HashMap<String, Schema>();

  public SchemaJsonParser() {
    for (Map.Entry<String, Type> entry : PRIMITIVES.entrySet()) {
      mSchemaMap.put(entry.getKey(), Schema.create(entry.getValue()));
    }
  }

  public Schema parse(String text) {
    try {
      return parse(OBJECT_MAPPER.readTree(text));
    } catch (IOException e) {
      throw new SchemaParseException(e);
    }
  }

  public Schema parse(JsonNode node) {
    parseNames(node, null);
    return parse(node, null);
  }

  private static Set<String> parseAliases(JsonNode node) {
    final JsonNode aliasesNode = node.get("aliases");
    final Set<String> aliases = new LinkedHashSet<String>();
    if (aliasesNode != null) {
      if (!aliasesNode.isArray()) {
        throw new SchemaParseException(
            "Schema descriptor field 'aliases' is not an array: " + node);
      }
      for (JsonNode aliasNode : aliasesNode) {
        if (!aliasNode.isTextual()) {
          throw new SchemaParseException("Alias is not a string: " + aliasNode);
        }
        aliases.add(aliasNode.getTextValue());
      }
    }
    return aliases;
  }

  private void parseNames(JsonNode jsonNode, String defaultNamespace) {
    if (jsonNode.isTextual()) {
      // No name to register.
    } else if (jsonNode.isObject()) {
      final String type = getRequiredText(jsonNode, "type",
          "Schema JSON descriptor has no 'type' field");
      if (NAMED_TYPES.contains(type)) {
        if (!jsonNode.has("namespace")) {
          final ObjectNode object = (ObjectNode) jsonNode;
          object.put("namespace", defaultNamespace);
        }
        final String namespace = getRequiredText(jsonNode, "namespace",
            "Schema JSON descriptor has no 'namespace' field");
        final String name = getRequiredText(jsonNode, "name",
            "Schema JSON descriptor has no 'name' field");
        final String doc = getOptionalText(jsonNode, "doc");
        final String fullName = namespace + "." + name;

        // Save the JSON descriptor for the schema:
        mJsonMap.put(fullName, jsonNode);

        if (type.equals("record") || type.equals("error")) {
          final boolean isError = type.equals("error");
          final Schema schema = Schema.createRecord(name, doc, namespace, isError);
          mSchemaMap.put(fullName, schema);
          for (String alias : parseAliases(jsonNode)) {
            final String fullAlias = alias.contains(".") ? alias : (namespace + "." + alias);
            mSchemaMap.put(fullAlias, schema);
          }

          // Process record fields minimally:
          // Extract the named types skeletons, but does not construct the fields/schemas.
          final JsonNode fieldsNode = jsonNode.get("fields");
          if (fieldsNode == null) {
            throw new SchemaParseException(
                "Record JSON descriptor has no 'fields' field: " + jsonNode);
          }
          if (!fieldsNode.isArray()) {
            throw new SchemaParseException(
                "Record JSON descriptor has invalid 'fields' field: " + jsonNode);
          }
          for (JsonNode fieldNode : fieldsNode) {
            final JsonNode fieldTypeNode = fieldNode.get("type");
            if (fieldTypeNode == null) {
              throw new SchemaParseException(
                  "Record field JSON descriptor has no 'type' field: " + fieldNode);
            }
            parseNames(fieldTypeNode, namespace);
          }
        }
      } else if (type.equals("array")) {
        final JsonNode itemsNode = jsonNode.get("items");
        if (itemsNode == null) {
          throw new SchemaParseException("Array JSON descriptor has no 'items' field: " + jsonNode);
        }
        parseNames(itemsNode, defaultNamespace);
      } else if (type.equals("map")) {
        final JsonNode valuesNode = jsonNode.get("values");
        if (valuesNode == null) {
          throw new SchemaParseException("Map JSON descriptor has no 'values' field: " + jsonNode);
        }
        parseNames(valuesNode, defaultNamespace);
      }
    } else if (jsonNode.isArray()) {
      // Process all the branches in the union:
      for (JsonNode branchNode : jsonNode) {
        parseNames(branchNode, defaultNamespace);
      }
    } else {
      throw new SchemaParseException("Invalid JSON schema: " + jsonNode);
    }
  }

  private Schema parse(JsonNode jsonNode, String defaultNS) {
    if (jsonNode.isTextual()) {
      return parseTextual(jsonNode.getTextValue(), defaultNS);
    } else if (jsonNode.isObject()) {
      return parseObject(jsonNode, defaultNS);
    } else if (jsonNode.isArray()) {
      return parseUnion(jsonNode, defaultNS);
    } else {
      throw new SchemaParseException("Invalid JSON schema: " + jsonNode);
    }
  }

  private Schema parseTextual(String text, String defaultNS) {
    if (PRIMITIVES.containsKey(text)) {
      return Schema.create(PRIMITIVES.get(text));
    }
    if (mSchemaMap.containsKey(text)) {
      return mSchemaMap.get(text);
    }
    throw new SchemaParseException("Invalid JSON schema: " + text);
  }

  private Schema parseObject(JsonNode schema, String defaultNS) {
    final String type = getRequiredText(schema, "type",
        "Schema JSON descriptor has no 'type' field");

    final String name = getOptionalText(schema,  "name");
    final String namespace = getOptionalText(schema,  "namespace");
    final String doc = getOptionalText(schema, "doc");

    Schema result = null;
    if (PRIMITIVES.containsKey(type)) {
      result = Schema.create(PRIMITIVES.get(type));

    } else if (type.equals("record") || type.equals("error")) {
      final List<Field> fields = new ArrayList<Field>();
      result = Schema.createRecord(name, doc, namespace, type.equals("error"));

      final JsonNode fieldsNode = schema.get("fields");
      if (fieldsNode == null || !fieldsNode.isArray()) {
        throw new SchemaParseException("Record has no 'fields' field: " + schema);
      }
      for (JsonNode fieldNode : fieldsNode) {
        final String fieldName = getRequiredText(fieldNode, "name",
            "Record field descriptor has no 'name' field: " + fieldNode);
        final String fieldDoc = getOptionalText(fieldNode, "doc");
        final JsonNode fieldTypeNode = fieldNode.get("type");
        if (fieldTypeNode == null) {
          throw new SchemaParseException("Record field descriptor has no 'type' field: " + fieldNode);
        }
        final Schema fieldSchema = parse(fieldTypeNode, namespace);
        Field.Order order = Field.Order.ASCENDING;
        final JsonNode orderNode = fieldNode.get("order");
        if (orderNode != null) {
          order = Field.Order.valueOf(orderNode.getTextValue().toUpperCase());
        }

        // Process default value:
        JsonNode defaultValue = fieldNode.get("default");
        if ((defaultValue != null)
            && (Type.FLOAT.equals(fieldSchema.getType())
                || Type.DOUBLE.equals(fieldSchema.getType()))
            && defaultValue.isTextual()) {
          defaultValue = new DoubleNode(Double.valueOf(defaultValue.getTextValue()));
        }

        final Field field =
            new Field(fieldName, fieldSchema, fieldDoc, defaultValue, order);

        // Load field properties:
        final Iterator<String> it = fieldNode.getFieldNames();
        while (it.hasNext()) {
          final String propName = it.next();
          if (!FIELD_RESERVED.contains(propName))
            field.addProp(propName, fieldNode.get(propName));
        }

//        // Process aliases:
//        f.aliases = parseAliases(field);

        fields.add(field);
      }
      result.setFields(fields);

    } else if (type.equals("enum")) {
      final JsonNode symbolsNode = schema.get("symbols");
      if ((symbolsNode == null) || !symbolsNode.isArray()) {
        throw new SchemaParseException("Enum has no symbols: "+schema);
      }
      LockableArrayList<String> symbols = new LockableArrayList<String>();
      for (JsonNode n : symbolsNode) {
        symbols.add(n.getTextValue());
      }
      result = Schema.createEnum(name, doc, namespace, symbols);

    } else if (type.equals("array")) {
      final JsonNode itemsNode = schema.get("items");
      if (itemsNode == null) {
        throw new SchemaParseException("Array has no items type: "+schema);
      }
      result = Schema.createArray(parse(itemsNode, defaultNS));

    } else if (type.equals("map")) {
      final JsonNode valuesNode = schema.get("values");
      if (valuesNode == null) {
        throw new SchemaParseException("Map has no values type: "+schema);
      }
      result = Schema.createMap(parse(valuesNode, defaultNS));

    } else if (type.equals("fixed")) {
      JsonNode sizeNode = schema.get("size");
      if (sizeNode == null || !sizeNode.isInt()) {
        throw new SchemaParseException("Invalid or no size: "+schema);
      }
      result = Schema.createFixed(name, doc, namespace, sizeNode.getIntValue());

    } else {
      throw new SchemaParseException("Invalid schema type: " + type);
    }

    // Load schema properties:
    final Iterator<String> it = schema.getFieldNames();
    while (it.hasNext()) {
      final String propName = it.next();
      if (!RESERVED_FIELDS.contains(propName)) {
        result.addProp(propName, schema.get(propName));
      }
    }

    // Register aliases?
//    if (result instanceof NamedSchema) {
//      Set<String> aliases = parseAliases(schema);
//      if (aliases != null)                      // add aliases
//        for (String alias : aliases)
//          result.addAlias(alias);
//    }

    return result;
  }

  private Schema parseUnion(JsonNode array, String defaultNS) {
    final LockableArrayList<Schema> types = new LockableArrayList<Schema>(array.size());
    for (JsonNode typeNode : array) {
      types.add(parse(typeNode, defaultNS));
    }
    return Schema.createUnion(types);
  }

  /**
   * Extracts text value associated to key from the container JsonNode.
   *
   * @param container Container where to find key.
   * @param key Key to look for in container.
   * @param error String to prepend to the SchemaParseException.
   * @return the text value.
   * @throws SchemaParseException if it doesn't exist.
   */
  private static String getRequiredText(JsonNode container, String key, String error) {
    final String text = getOptionalText(container, key);
    if (null == text) {
      throw new SchemaParseException(error + ": " + container);
    }
    return text;
  }

  /** Extracts text value associated to key from the container JsonNode. */
  private static String getOptionalText(JsonNode container, String key) {
    final JsonNode node = container.get(key);
    return (node != null) ? node.getTextValue() : null;
  }
}
