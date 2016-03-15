require_relative '../lib/writer'

require 'aws-sdk'

RSpec.describe Writer do
  connection_info = {:region => 'us-east-1', :endpoint => 'http://localhost:8000'}

  it 'saves a rich document to DynamoDB' do
    item = {
        'k1': 'v1',
        'k2': 23,
        'k3': true,
        'k4': [1, 2, 3],
        'k5': {
            'k51': 'abc',
            'k52': 'def'
        }
    }
    Writer.new(connection_info).save(item)

    client = Aws::DynamoDB::Client.new(connection_info)
    response = client.get_item({
                                   table_name: 'foo',
                                   key: {
                                       'k1': 'v1'
                                   }

                               })

    expect(response.item[:k1]).to eql(item['k1'])
    expect(response.item[:k2]).to eql(item['k2'])
  end
end