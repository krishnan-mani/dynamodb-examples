require 'aws-sdk'

require_relative '../lib/writer'

RSpec.describe 'conditional put_item operations' do

  connection_info = {:region => 'us-east-1', :endpoint => 'http://localhost:8000'}

  before(:each) do
    client = Aws::DynamoDB::Client.new(connection_info)
    existing_tables = client.list_tables.table_names

    tables = ['foo']
    tables.each do |table|
      if existing_tables.include?(table)
        client.delete_table({
                                table_name: table
                            })
      end
    end
  end

  it "add a new item if one with the specified primary key doesn't exist" do
    some_key = SecureRandom.uuid
    item = {'k1': some_key, 'k2': 'y'}
    Writer.new(connection_info).save_foo(item)

    client = Aws::DynamoDB::Client.new(connection_info)
    response = client.get_item({
                                   table_name: 'foo',
                                   key: {
                                       'k1': some_key
                                   }
                               })

    expect(response.item).not_to be_nil
    expect(response.item['k1']).to eql(some_key)

    expect {
      client.put_item({
                          table_name: 'foo',
                          item: item,
                          condition_expression: "attribute_not_exists(k1)"
                      })
    }.to raise_error(Aws::DynamoDB::Errors::ConditionalCheckFailedException, 'The conditional request failed')
  end

end