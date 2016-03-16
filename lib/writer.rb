require 'aws-sdk'

class Writer

  def initialize(connection_info)
    @client = Aws::DynamoDB::Client.new(connection_info)
  end

  def save_shoes(*shoes)
    ensure_shoes_table
    shoes.each do |shoe|
      @client.put_item({
                           table_name: 'shoes',
                           item: shoe
                       })
    end
  end

  def save_product(item)
    ensure_products_table
    @client.put_item({
                         table_name: 'products',
                         item: item
                     })
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

  def save_xyz(*items)
    ensure_xyz_table
    items.each do |item|
      @client.put_item({
                           table_name: 'xyz',
                           item: item
                       })
    end
  end

  private

  def table_exists?(table_name)
    @client.list_tables.table_names.include?(table_name)
  end

  def ensure_shoes_table
    unless table_exists?('shoes')
      @client.create_table({
                               table_name: 'shoes',
                               attribute_definitions: [
                                   {
                                       attribute_name: 'Id',
                                       attribute_type: 'N'
                                   },
                                   {
                                       attribute_name: 'Brand',
                                       attribute_type: 'S'
                                   }
                               ],
                               key_schema: [
                                   {
                                       attribute_name: 'Id',
                                       key_type: 'HASH'
                                   },
                                   {
                                       attribute_name: 'Brand',
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

  def ensure_products_table
    unless table_exists?('products')
      @client.create_table({
                               table_name: 'products',
                               attribute_definitions: [
                                   {
                                       attribute_name: 'a866',
                                       attribute_type: 'N'
                                   }
                               ],
                               key_schema: [
                                   {
                                       attribute_name: 'a866',
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

  def ensure_xyz_table
    unless table_exists?('xyz')
      @client.create_table({
                               table_name: 'xyz',
                               attribute_definitions: [
                                   {
                                       attribute_name: 'k1',
                                       attribute_type: 'S'
                                   },
                                   {
                                       attribute_name: 'k2',
                                       attribute_type: 'N'
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

  def ensure_foo_table
    unless table_exists?('foo')
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
    unless table_exists?('bar')
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