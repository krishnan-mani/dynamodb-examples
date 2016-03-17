require_relative '../lib/writer'

require 'aws-sdk'

RSpec.describe 'batch_get_item operation' do

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


  it 'fetches items from multiple tables using batch_get_item' do
    writer = Writer.new(connection_info)
    foo_items = [{
                     'k1': 'x1', 'j1': 'jam'
                 },
                 {
                     'k1': 'x2', 'x2': 'marmalade'
                 }]
    writer.save_foo(*foo_items)

    kar_items = [{
                     'k2': 'jim', 'x1': 'bonbon'
                 },
                 {
                     'k2': 'beam', 'x1': 'martin'
                 }]
    writer.save_kar(*kar_items)

    client = Aws::DynamoDB::Client.new(connection_info)
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

    Writer.new(connection_info).save_foo(*one_hundred_and_one_items)

    client = Aws::DynamoDB::Client.new(connection_info)
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