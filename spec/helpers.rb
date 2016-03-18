def connection_info
  {:region => 'us-east-1', :endpoint => 'http://localhost:8000'}
end

def delete_table(table_name)
  client = Aws::DynamoDB::Client.new(connection_info)
  if client.list_tables.table_names.include?(table_name)
    client.delete_table({
                            table_name: table_name
                        })
  end
end

def recreate_table(table_name, hash_key, sort_key = nil)
  client = Aws::DynamoDB::Client.new(connection_info)
  delete_table(table_name)
  client.create_table(get_table_definition(table_name, hash_key, sort_key))
end

def get_table_definition(table_name, hash_key, sort_key = nil)
  table_definition = {
      table_name: table_name
  }

  attribute_definitions = [{
                               attribute_name: hash_key,
                               attribute_type: 'S'
                           }]

  key_schema = [
      {
          attribute_name: hash_key,
          key_type: 'HASH'
      }
  ]

  if sort_key
    attribute_definitions << {
        attribute_name: sort_key,
        attribute_type: 'S'
    }

    key_schema << {
        attribute_name: sort_key,
        key_type: 'RANGE'
    }
  end

  table_definition[:attribute_definitions] = attribute_definitions
  table_definition[:key_schema] = key_schema
  table_definition[:provisioned_throughput] = {
      read_capacity_units: 1,
      write_capacity_units: 1
  }

  table_definition.dup
end
