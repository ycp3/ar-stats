module ArStats
  module Model
    def ar_stats(*column_names)
      disallow_raw_sql!(column_names)

      column_names.map!(&:to_s)
      if column_names.any?
        check_column_names(*column_names)
      else
        column_names = text_columns
      end

      stats = HashWithIndifferentAccess.new
      column_names.each do |column|
        stats[column] = {
          blank_count: blank_count(column),
          max_length: max_length(column),
          min_length: min_length(column),
          avg_length: avg_length(column),
          min_length_samples: min_length_samples(column),
          max_length_samples: max_length_samples(column)
        }
      end

      stats
    end

    def text_columns
      columns.select { |column| (column.type == :string || column.type == :text) && !column.sql_type_metadata.sql_type.end_with?("[]") }.map(&:name)
    end

    def blank_count(column)
      disallow_raw_sql!([column])
      check_column_names(column)
      relation.where(column => [nil, ""]).count
    end

    def max_length(column)
      disallow_raw_sql!([column])
      check_column_names(column)
      relation.pick(Arel.sql("max(length(#{column}))"))
    end

    def min_length(column)
      disallow_raw_sql!([column])
      check_column_names(column)
      relation.where.not(column => "").pick(Arel.sql("min(length(#{column}))"))
    end

    def avg_length(column)
      disallow_raw_sql!([column])
      check_column_names(column)
      relation.pick(Arel.sql("avg(length(#{column}))"))
    end

    def min_length_samples(column, n = 5)
      disallow_raw_sql!([column])
      check_column_names(column)
      relation.where("length(#{column}) = #{min_length(column)}").take(n).pluck(column)
    end

    def max_length_samples(column, n = 5)
      disallow_raw_sql!([column])
      check_column_names(column)
      relation.where("length(#{column}) = #{max_length(column)}").take(n).pluck(column)
    end

    def avg_length_samples(column, n = 5)
      disallow_raw_sql!([column])
      check_column_names(column)
      relation.where("length(#{column}) = #{avg_length(column)}").take(n).pluck(column)
    end

    private

    def check_column_names(*column_names)
      column_names.map!(&:to_s)
      unknown_columns = (column_names - columns.map(&:name))
      raise ArgumentError.new("Unknown column names: #{unknown_columns.join(", ")}.") if unknown_columns.any?

      incompatible_columns = (column_names - text_columns)
      raise ArgumentError.new("Arguments must be valid columns of type string or text. Incompatible columns: #{incompatible_columns.join(", ")}.") if incompatible_columns.any?
    end
  end
end
