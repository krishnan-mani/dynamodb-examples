require 'aws-sdk-dynamodb'

RSpec.describe 'batch_get_item operation' do

  it 'fetches items from multiple tables using batch_get_item' do
    foo_items = [{
                     'k1': 'x1', 'j1': 'jam'
                 },
                 {
                     'k1': 'x2', 'x2': 'marmalade'
                 }]

    recreate_table('foo', 'k1', 'S')
    client = Aws::DynamoDB::Client.new(connection_info)
    foo_items.each do |item|
      client.put_item({
                          table_name: 'foo',
                          item: item
                      })
    end

    kar_items = [{
                     'k2': 'jim', 'x1': 'bonbon'
                 },
                 {
                     'k2': 'beam', 'x1': 'martin'
                 }]

    recreate_table('kar', 'k2', 'S', 'x1', 'S')
    kar_items.each do |item|
      client.put_item({
                          table_name: 'kar',
                          item: item
                      })
    end

    response = client.batch_get_item({
                                         request_items: {
                                             'foo': {
                                                 keys: [
                                                     {
                                                         'k1': 'x1'
                                                     },
                                                     {
                                                         'k1': 'x2'
                                                     }
                                                 ]
                                             },
                                             'kar': {
                                                 keys: [
                                                     {
                                                         'k2': 'beam',
                                                         'x1': 'martin'
                                                     }
                                                 ]

                                             }
                                         }
                                     })

    expect(response.responses['foo']).not_to be_nil
    expect(response.responses['foo'].length).to eql(2)
    jam_items = response.responses['foo'].select { |x|
      x['k1'] == 'x1'
    }
    expect(jam_items.length).to eql(1)
    expect(jam_items.first['j1']).to eql('jam')

    expect(response.responses['kar']).not_to be_nil
    expect(response.responses['kar'].length).to eql(1)
    martin_items = response.responses['kar'].select { |y|
      y['k2'] == 'beam'
    }
    expect(martin_items.length).to eql(1)
    expect(martin_items.first['x1']).to eql('martin')
  end

  it "If you request more than 100 items BatchGetItem will return a ValidationException with the message 'Too many items requested for the BatchGetItem call'" do
    one_hundred_and_one_items = (1..101).collect do |idx|
      {'k1': "X#{idx}", 'idx': idx}
    end

    key_expression_list = (1..101).collect do |idx|
      {'k1': "X#{idx}"}
    end

    recreate_table('foo', 'k1', 'S')
    client = Aws::DynamoDB::Client.new(connection_info)
    one_hundred_and_one_items.each do |item|
      client.put_item({
                     table_name: 'foo',
                     item: item
                 })
    end


    expect {
      client.batch_get_item({
                                request_items: {
                                    'foo': {
                                        keys: key_expression_list
                                    }
                                }
                            })
    }.to raise_error(Aws::DynamoDB::Errors::ValidationException, 'Too many items requested for the BatchGetItem call')
  end

end