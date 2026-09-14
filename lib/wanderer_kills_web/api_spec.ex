defmodule WandererKillsWeb.ApiSpec do
  @moduledoc """
  OpenAPI specification for Wanderer Kills API.

  Provides a complete OpenAPI 3.0 specification for all API endpoints,
  including REST endpoints, SSE streaming, and WebSocket connections.
  """

  alias OpenApiSpex.{Components, Info, OpenApi, Paths, Server}
  alias WandererKillsWeb.Router

  @behaviour OpenApi

  @impl OpenApi
  def spec do
    %OpenApi{
      servers: [
        # Use static server config instead of endpoint to avoid startup dependency
        %Server{
          url: "http://localhost:4004",
          description: "Development server"
        }
      ],
      info: %Info{
        title: "Wanderer Kills API",
        version: "1.0.0",
        description: """
        Real-time EVE Online killmail data service.

        This API provides access to killmail data from EVE Online with various integration options:
        - REST endpoints for fetching historical data
        - Server-Sent Events (SSE) for real-time streaming
        - WebSocket connections for bidirectional communication
        - Webhook subscriptions for push notifications

        All killmail data is enriched with additional information from EVE's ESI API.
        """
      },
      paths: Paths.from_router(Router),
      components: %Components{
        schemas: %{
          "Killmail" => killmail_schema(),
          "Victim" => victim_schema(),
          "Attacker" => attacker_schema(),
          "ZKB" => zkb_schema(),
          "Error" => error_schema(),
          "HealthStatus" => health_status_schema(),
          "Subscription" => subscription_schema(),
          "SSEEvent" => sse_event_schema()
        }
      }
    }
    |> OpenApiSpex.resolve_schema_modules()
  end

  defp killmail_schema do
    %OpenApiSpex.Schema{
      title: "Killmail",
      description: "A complete killmail record with enriched data",
      type: :object,
      properties: %{
        killmail_id: %OpenApiSpex.Schema{type: :integer, description: "Unique killmail ID"},
        kill_time: %OpenApiSpex.Schema{
          type: :string,
          format: :"date-time",
          description: "Time when the kill occurred"
        },
        solar_system_id: %OpenApiSpex.Schema{type: :integer, description: "EVE system ID"},
        solar_system_name: %OpenApiSpex.Schema{type: :string, description: "System name"},
        victim: %OpenApiSpex.Reference{"$ref": "#/components/schemas/Victim"},
        attackers: %OpenApiSpex.Schema{
          type: :array,
          items: %OpenApiSpex.Reference{"$ref": "#/components/schemas/Attacker"}
        },
        zkb: %OpenApiSpex.Reference{"$ref": "#/components/schemas/ZKB"}
      },
      required: [:killmail_id, :kill_time, :solar_system_id, :victim, :attackers],
      example: %{
        "killmail_id" => 123_456_789,
        "kill_time" => "2024-01-15T14:30:00Z",
        "solar_system_id" => 30_000_142,
        "solar_system_name" => "Jita",
        "victim" => %{
          "character_id" => 987_654_321,
          "character_name" => "Victim Name",
          "ship_type_id" => 670,
          "ship_name" => "Raven"
        },
        "attackers" => [
          %{
            "character_id" => 123_456_789,
            "character_name" => "Attacker Name",
            "ship_type_id" => 17_918,
            "ship_name" => "Rattlesnake",
            "final_blow" => true
          }
        ]
      }
    }
  end

  defp victim_schema do
    %OpenApiSpex.Schema{
      title: "Victim",
      description: "The victim of a killmail",
      type: :object,
      properties: %{
        character_id: %OpenApiSpex.Schema{type: :integer, description: "Character ID"},
        character_name: %OpenApiSpex.Schema{type: :string, description: "Character name"},
        corporation_id: %OpenApiSpex.Schema{type: :integer, description: "Corporation ID"},
        corporation_name: %OpenApiSpex.Schema{type: :string, description: "Corporation name"},
        alliance_id: %OpenApiSpex.Schema{
          type: :integer,
          description: "Alliance ID",
          nullable: true
        },
        alliance_name: %OpenApiSpex.Schema{
          type: :string,
          description: "Alliance name",
          nullable: true
        },
        ship_type_id: %OpenApiSpex.Schema{type: :integer, description: "Ship type ID"},
        ship_name: %OpenApiSpex.Schema{type: :string, description: "Ship name"},
        damage_taken: %OpenApiSpex.Schema{type: :integer, description: "Total damage taken"}
      },
      required: [:character_id, :ship_type_id, :damage_taken]
    }
  end

  defp attacker_schema do
    %OpenApiSpex.Schema{
      title: "Attacker",
      description: "An attacker on a killmail",
      type: :object,
      properties: %{
        character_id: %OpenApiSpex.Schema{type: :integer, description: "Character ID"},
        character_name: %OpenApiSpex.Schema{type: :string, description: "Character name"},
        corporation_id: %OpenApiSpex.Schema{type: :integer, description: "Corporation ID"},
        corporation_name: %OpenApiSpex.Schema{type: :string, description: "Corporation name"},
        alliance_id: %OpenApiSpex.Schema{
          type: :integer,
          description: "Alliance ID",
          nullable: true
        },
        alliance_name: %OpenApiSpex.Schema{
          type: :string,
          description: "Alliance name",
          nullable: true
        },
        ship_type_id: %OpenApiSpex.Schema{type: :integer, description: "Ship type ID"},
        ship_name: %OpenApiSpex.Schema{type: :string, description: "Ship name"},
        weapon_type_id: %OpenApiSpex.Schema{type: :integer, description: "Weapon type ID"},
        weapon_name: %OpenApiSpex.Schema{type: :string, description: "Weapon name"},
        damage_done: %OpenApiSpex.Schema{type: :integer, description: "Damage dealt"},
        final_blow: %OpenApiSpex.Schema{type: :boolean, description: "Dealt final blow"}
      },
      required: [:damage_done, :final_blow]
    }
  end

  defp zkb_schema do
    %OpenApiSpex.Schema{
      title: "ZKB",
      description: "zKillboard metadata",
      type: :object,
      properties: %{
        location_id: %OpenApiSpex.Schema{type: :integer, description: "Location ID"},
        hash: %OpenApiSpex.Schema{type: :string, description: "Kill hash"},
        fitted_value: %OpenApiSpex.Schema{type: :number, description: "Fitted modules value"},
        total_value: %OpenApiSpex.Schema{type: :number, description: "Total ISK value"},
        points: %OpenApiSpex.Schema{type: :integer, description: "Kill points"},
        npc: %OpenApiSpex.Schema{type: :boolean, description: "NPC kill"},
        solo: %OpenApiSpex.Schema{type: :boolean, description: "Solo kill"},
        awox: %OpenApiSpex.Schema{type: :boolean, description: "Friendly fire"}
      }
    }
  end

  defp error_schema do
    %OpenApiSpex.Schema{
      title: "Error",
      description: "Error response",
      type: :object,
      properties: %{
        error: %OpenApiSpex.Schema{
          type: :object,
          properties: %{
            type: %OpenApiSpex.Schema{type: :string, description: "Error type"},
            subtype: %OpenApiSpex.Schema{type: :string, description: "Error subtype"},
            message: %OpenApiSpex.Schema{type: :string, description: "Error message"},
            details: %OpenApiSpex.Schema{
              type: :object,
              description: "Additional error details",
              nullable: true
            }
          },
          required: [:type, :message]
        }
      },
      required: [:error],
      example: %{
        "error" => %{
          "type" => "validation_error",
          "subtype" => "invalid_format",
          "message" => "Invalid system_ids parameter"
        }
      }
    }
  end

  defp health_status_schema do
    %OpenApiSpex.Schema{
      title: "HealthStatus",
      description: "Health check response",
      type: :object,
      properties: %{
        status: %OpenApiSpex.Schema{
          type: :string,
          enum: ["healthy", "degraded", "unhealthy"],
          description: "Overall health status"
        },
        components: %OpenApiSpex.Schema{
          type: :object,
          description: "Individual component health",
          additionalProperties: %OpenApiSpex.Schema{
            type: :object,
            properties: %{
              status: %OpenApiSpex.Schema{type: :string},
              message: %OpenApiSpex.Schema{type: :string, nullable: true}
            }
          }
        },
        timestamp: %OpenApiSpex.Schema{type: :string, format: :"date-time"}
      },
      required: [:status, :components, :timestamp]
    }
  end

  defp subscription_schema do
    %OpenApiSpex.Schema{
      title: "Subscription",
      description: "Webhook subscription",
      type: :object,
      properties: %{
        id: %OpenApiSpex.Schema{type: :string, description: "Subscription ID"},
        subscriber_id: %OpenApiSpex.Schema{type: :string, description: "Subscriber identifier"},
        system_ids: %OpenApiSpex.Schema{
          type: :array,
          items: %OpenApiSpex.Schema{type: :integer},
          description: "Subscribed system IDs"
        },
        character_ids: %OpenApiSpex.Schema{
          type: :array,
          items: %OpenApiSpex.Schema{type: :integer},
          description: "Tracked character IDs"
        },
        callback_url: %OpenApiSpex.Schema{type: :string, description: "Webhook URL"},
        created_at: %OpenApiSpex.Schema{type: :string, format: :"date-time"}
      },
      required: [:id, :subscriber_id, :callback_url, :created_at]
    }
  end

  defp sse_event_schema do
    %OpenApiSpex.Schema{
      title: "SSEEvent",
      description: "Server-Sent Event formats",
      oneOf: [
        %OpenApiSpex.Schema{
          type: :object,
          properties: %{
            event: %OpenApiSpex.Schema{type: :string, enum: ["connected"]},
            data: %OpenApiSpex.Schema{type: :string, enum: ["SSE stream connected"]}
          }
        },
        %OpenApiSpex.Schema{
          type: :object,
          properties: %{
            event: %OpenApiSpex.Schema{type: :string, enum: ["killmail"]},
            data: %OpenApiSpex.Reference{"$ref": "#/components/schemas/Killmail"},
            id: %OpenApiSpex.Schema{type: :string}
          }
        },
        %OpenApiSpex.Schema{
          type: :object,
          properties: %{
            event: %OpenApiSpex.Schema{type: :string, enum: ["heartbeat"]},
            data: %OpenApiSpex.Schema{
              type: :object,
              properties: %{
                timestamp: %OpenApiSpex.Schema{type: :string, format: :"date-time"}
              }
            }
          }
        }
      ]
    }
  end
end
