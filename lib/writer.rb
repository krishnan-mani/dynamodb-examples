require 'aws-sdk'

class Writer

  def initialize(connection_info)
    @client = Aws::DynamoDB::Client.new(connection_info)
    ensure_tables
  end

  def save(item)
    @client.put_item({
                         table_name: 'foo',
                         item: item
                     })
  end

  private

  def ensure_tables
    tables = @client.list_tables.table_names
    unless tables.include?('foo')
      @client.create_table({
                               table_name: 'foo',
                               attribute_definitions: [
                                   {
                                       attribute_name: 'k1',
                                       attribute_type: 'S'
                                   }
                               ],
                               key_schema: [
                                   {
                                       attribute_name: 'k1',
                                       key_type: 'HASH'
                                   }
                               ],
                               provisioned_throughput: {
                                   read_capacity_units: 1,
                                   write_capacity_units: 1
                               }
                           })
    end
  end

end