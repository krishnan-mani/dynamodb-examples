require_relative '../lib/writer'

require 'aws-sdk'

RSpec.describe 'table with partition (hash) key and sort (range) key' do
  connection_info = {:region => 'us-east-1', :endpoint => 'http://localhost:8000'}

  before(:each) do
    client = Aws::DynamoDB::Client.new(connection_info)
    tables = client.list_tables.table_names
    tables.each do |table|
      client.delete_table({
                              table_name: table
                          })
    end
  end


  it 'saves multiple items by hash and range keys to DynamoDB' do
    items = [{k1: 'a1', k2: 'a2'}, {k1: 'a1', k2: 'y'}]
    Writer.new(connection_info).save_bar(*items)

    client = Aws::DynamoDB::Client.new(connection_info)
    response = client.query({
                                table_name: 'bar',
                                select: 'COUNT',
                                key_condition_expression: 'k1 = :v_k1',
                                expression_attribute_values: {
                                    ':v_k1': 'a1'
                                }
                            })
    expect(response.count).to eql(2)
  end

  it 'supports querying using hash key and comparison on range key using begins_with' do
    items = [
        {
            Id: 206,
            Brand: 'Reebok',
            Size: 8
        },
        {
            Id: 206,
            Brand: 'Nike'
        },
        {
            Id: 206,
            Brand: 'Ni Hao'
        }
    ]
    Writer.new(connection_info).save_shoes(*items)

    client = Aws::DynamoDB::Client.new(connection_info)
    response = client.query({
                                table_name: 'shoes',
                                key_condition_expression: 'Id = :v_id AND begins_with(Brand, :v_brand)',
                                expression_attribute_values: {
                                    ':v_id': 206,
                                    ':v_brand': 'Ni'
                                }
                            })

    expect(response.items).not_to be_nil
    expect(response.count).to eql(2)
  end

end
