require_relative '../lib/writer'

require 'aws-sdk'

require 'byebug'

RSpec.describe Writer do
  connection_info = {:region => 'us-east-1', :endpoint => 'http://localhost:8000'}

  it 'saves a rich document by hash key to DynamoDB' do
    item = {
        k1: 'v1',
        k2: 23,
        k3: true,
        k4: [1, 2, 3],
        k5: {
            'k51': 'abc',
            'k52': 'def'
        }
    }
    Writer.new(connection_info).save_foo(item)

    client = Aws::DynamoDB::Client.new(connection_info)
    response = client.get_item({
                                   table_name: 'foo',
                                   key: {
                                       k1: 'v1'
                                   }

                               })

    expect(response.item[:k1]).to eql(item['k1'])
    expect(response.item[:k2]).to eql(item['k2'])
  end

  it 'saves multiple documents by hash and range keys to DynamoDB' do
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
end