require 'aws-sdk'

class Writer

  def initialize(connection_info)
    @client = Aws::DynamoDB::Client.new(connection_info)
  end

  def save_foo(*items)
    ensure_foo_table
    items.each do |item|
      @client.put_item({
                           table_name: 'foo',
                           item: item
                       })
    end
  end

  def save_bar(*items)
    ensure_bar_table
    items.each do |item|
      @client.put_item({
                           table_name: 'bar',
                           item: item
                       })
    end
  end

  private

  def ensure_foo_table
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

  def ensure_bar_table
    tables = @client.list_tables.table_names
    unless tables.include?('bar')
      @client.create_table({
                               table_name: 'bar',
                               attribute_definitions: [
                                   {
                                       attribute_name: 'k1',
                                       attribute_type: 'S'
                                   },
                                   {
                                       attribute_name: 'k2',
                                       attribute_type: 'S'
                                   }
                               ],
                               key_schema: [
                                   {
                                       attribute_name: 'k1',
                                       key_type: 'HASH'
                                   },
                                   {
                                       attribute_name: 'k2',
                                       key_type: 'RANGE'
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