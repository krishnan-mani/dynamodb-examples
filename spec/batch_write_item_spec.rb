require 'aws-sdk'

require_relative '../lib/writer'

RSpec.describe 'batch_write_item' do

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

  it 'puts multiple items in one table' do
    magnus = {
        'k1': 'Magnus',
        'k2': 'Carlsen'
    }
    vishy = {
        'k1': 'Vishy',
        'k2': 'Anand'
    }
    svidler = {
        'k1': 'Peter',
        'k2': 'Svidler'
    }

    Writer.new(connection_info).save_foo(magnus)

    client = Aws::DynamoDB::Client.new(connection_info)
    response = client.batch_write_item({
                                           request_items: {
                                               'foo': [
                                                   {put_request: {item: vishy}},
                                                   {put_request: {item: svidler}}
                                               ]
                                           }
                                       })
    expect(response.unprocessed_items).to be_empty

    get_item_response = client.get_item({
                                            table_name: 'foo',
                                            key: {
                                                'k1': 'Vishy'
                                            }
                                        })
    expect(get_item_response.item).not_to be_nil
    expect(get_item_response.item[:k1]).to eql(vishy['k1'])

    get_item_response = client.get_item({
                                            table_name: 'foo',
                                            key: {
                                                'k1': 'Peter'
                                            }
                                        })
    expect(get_item_response.item).not_to be_nil
    expect(get_item_response.item[:k1]).to eql(svidler['k1'])
  end

  it 'puts multiple items in more than one table' do
    :pending
  end

  it 'deletes multiple items in one table' do
    :pending
  end

  it 'deletes multiple items in more than one table' do
    :pending
  end

end