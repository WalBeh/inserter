defmodule CrateWrite.Generator do
  @moduledoc "Record generator with pre-cached strings for maximum throughput."

  @regions ["us-east", "us-west", "eu-central", "ap-southeast"]
  @product_categories ["electronics", "books", "clothing", "home", "sports"]
  @event_types ["view", "click", "purchase", "cart_add", "cart_remove"]
  @user_segments ["premium", "standard", "basic", "trial"]

  # Pre-cached strings — no string interpolation in hot path
  @browsers Enum.map(1..5, &"Browser-#{&1}")
  @oses Enum.map(1..3, &"OS-#{&1}")
  @referrers Enum.map(1..10, &"ref-#{&1}")

  defstruct [:object_values, :num_objects]

  def new(num_objects \\ 0) do
    object_values =
      if num_objects > 0 do
        for i <- 0..(num_objects - 1), into: %{} do
          cardinality = Enum.random(3..8)
          vals = for j <- 0..(cardinality - 1), do: "obj#{i}_val_#{j}"
          {i, vals}
        end
      else
        %{}
      end

    %__MODULE__{
      object_values: object_values,
      num_objects: num_objects
    }
  end

  def generate_record(%__MODULE__{} = gen) do
    metadata = %{
      "browser" => Enum.random(@browsers),
      "os" => Enum.random(@oses),
      "session_id" => UUID.uuid4(),
      "page_views" => Enum.random(1..20),
      "referrer" => Enum.random(@referrers)
    }

    base = [
      UUID.uuid4(),
      DateTime.utc_now() |> DateTime.to_iso8601(),
      Enum.random(@regions),
      Enum.random(@product_categories),
      Enum.random(@event_types),
      Enum.random(1..10_000),
      Enum.random(@user_segments),
      Float.round(:rand.uniform() * 999.0 + 1.0, 2),
      Enum.random(1..100),
      metadata
    ]

    if gen.num_objects > 0 do
      objects =
        for i <- 0..(gen.num_objects - 1) do
          vals = Map.get(gen.object_values, i, ["default_obj_#{i}"])
          Enum.random(vals)
        end

      base ++ objects
    else
      base
    end
  end

  def generate_batch(%__MODULE__{} = gen, batch_size) do
    for _ <- 1..batch_size, do: generate_record(gen)
  end
end
