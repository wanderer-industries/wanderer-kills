defmodule WandererKills.OpenApiAssertions do
  @moduledoc false

  import ExUnit.Assertions

  def assert_resolved_spec(spec) do
    operation = spec["paths"]["/api/v1/kills/systems"]["post"]

    assert %{"$ref" => "#/components/schemas/BulkKillsResponse"} =
             operation["responses"]["200"]["content"]["application/json"]["schema"]

    assert %{"$ref" => "#/components/schemas/Error"} =
             operation["responses"]["400"]["content"]["application/json"]["schema"]

    assert %{
             "type" => "object",
             "required" => ["system_ids"],
             "properties" => %{
               "system_ids" => %{"type" => "array", "items" => %{"type" => "integer"}}
             }
           } = operation["requestBody"]["content"]["application/json"]["schema"]

    schemas = spec["components"]["schemas"]

    assert %{"$ref" => "#/components/schemas/Killmail"} =
             schemas["BulkKillsResponse"]["properties"]["systems_kills"]["additionalProperties"][
               "items"
             ]

    # Existing manual components win title collisions; resolution must not
    # silently replace them with the differing schema-module definitions.
    assert %{"type" => "string"} =
             schemas["Error"]["properties"]["error"]["properties"]["subtype"]

    refute Map.has_key?(schemas["Error"]["properties"]["error"]["properties"], "domain")
    refute "zkb" in schemas["Killmail"]["required"]

    Enum.each(schemas, fn {_name, schema} -> assert_schema(schema, schemas) end)
    assert_document_schemas(spec["paths"], schemas)
  end

  defp assert_document_schemas(value, schemas) when is_map(value) do
    Enum.each(value, fn
      {"schema", schema} -> assert_schema(schema, schemas)
      {key, _value} when key in ["example", "examples"] -> :ok
      {_key, nested} -> assert_document_schemas(nested, schemas)
    end)
  end

  defp assert_document_schemas(value, schemas) when is_list(value) do
    Enum.each(value, &assert_document_schemas(&1, schemas))
  end

  defp assert_document_schemas(_value, _schemas), do: :ok

  defp assert_schema(schema, schemas) do
    assert is_map(schema), "expected a JSON schema object, got: #{inspect(schema)}"

    Enum.each(schema, fn
      {"$ref", "#/components/schemas/" <> name} ->
        assert is_map(schemas[name]), "missing component schema: #{name}"

      {"properties", properties} ->
        Enum.each(properties, fn {_name, property} -> assert_schema(property, schemas) end)

      {key, nested} when key in ["items", "not"] ->
        assert_schema(nested, schemas)

      {"additionalProperties", nested} when is_map(nested) ->
        assert_schema(nested, schemas)

      {key, nested} when key in ["allOf", "anyOf", "oneOf"] ->
        Enum.each(nested, &assert_schema(&1, schemas))

      _ ->
        :ok
    end)
  end
end
