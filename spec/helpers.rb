def connection_info
  {
      :access_key_id => 'some_aki', :secret_access_key => 'some_sak',
      :region => 'us-east-1',
      :endpoint => 'http://localhost:8000'
  }
end

def table_exists?(table_name)
  client = Aws::DynamoDB::Client.new(connection_info)
  client.list_tables.table_names.include?(table_name)
end

def delete_table(*table_names)
  client = Aws::DynamoDB::Client.new(connection_info)
  table_names.each do |table_name|
    if table_exists?(table_name)
      client.delete_table({
                              table_name: table_name
                          })
    end
  end
end

def recreate_table(table_name, hash_key, hash_key_type, sort_key = nil, sort_key_type = nil)
  client = Aws::DynamoDB::Client.new(connection_info)
  delete_table(table_name)
  client.create_table(get_table_definition(table_name, hash_key, hash_key_type, sort_key, sort_key_type))
end

def get_table_definition(table_name, hash_key, hash_key_type, sort_key = nil, sort_key_type = nil)
  if sort_key
    raise InvalidArgumentError, 'Specify a type for sort key' unless sort_key_type
  end

  table_definition = {
      table_name: table_name
  }

  attribute_definitions = [{
                               attribute_name: hash_key,
                               attribute_type: hash_key_type
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
        attribute_type: sort_key_type
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
