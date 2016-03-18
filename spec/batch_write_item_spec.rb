require 'aws-sdk'

RSpec.describe 'batch_write_item' do

  before(:each) do
    delete_table('foo', 'chess_players', 'cricketers')
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
    karjakin = {
        'k1': 'Sergey',
        'k2': 'Karjakin'
    }
    caruana = {
        'k1': 'Fabiano',
        'k2': 'Caruana'
    }

    rohit = {
        'fn': 'Rohit',
        'ln': 'Sharma'
    }

    client = Aws::DynamoDB::Client.new(connection_info)
    client.create_table(get_table_definition('chess_players', 'k1', 'S'))
    client.create_table(get_table_definition('cricketers', 'fn', 'S'))

    response = client.batch_write_item({
                                           request_items: {
                                               'chess_players': [
                                                   {put_request: {item: karjakin}},
                                                   {put_request: {item: caruana}}
                                               ],
                                               'cricketers': [
                                                   {put_request: {item: rohit}}
                                               ]
                                           }
                                       })

    expect(response.unprocessed_items).to be_empty

    get_chess_players_response = client.get_item({
                                                     table_name: 'chess_players',
                                                     key: {
                                                         'k1': 'Sergey'
                                                     }
                                                 })
    expect(get_chess_players_response.item).not_to be_nil
    expect(get_chess_players_response.item['k2']).to eql('Karjakin')

    get_cricketers_response = client.get_item({
                                                  table_name: 'cricketers',
                                                  key: {
                                                      'fn': 'Rohit'
                                                  }
                                              })
    expect(get_cricketers_response.item).not_to be_nil
    expect(get_cricketers_response.item['ln']).to eql('Sharma')
  end

  it 'deletes multiple items in one table' do
    topalov = {
        'k1': 'Veselin',
        'k2': 'Topalov'
    }
    giri = {
        'k1': 'Anish',
        'k2': 'Giri'
    }

    client = Aws::DynamoDB::Client.new(connection_info)
    client.create_table(get_table_definition('chess_players', 'k1', 'S'))
    client.put_item({
                        table_name: 'chess_players',
                        item: topalov
                    })
    client.put_item({
                        table_name: 'chess_players',
                        item: giri
                    })

    get_item_response = client.get_item({
                                            table_name: 'chess_players',
                                            key: {
                                                'k1': 'Veselin'
                                            }
                                        })
    expect(get_item_response.item).not_to be_nil
    expect(get_item_response.item['k1']).to eql('Veselin')

    client.batch_write_item({
                                request_items: {
                                    'chess_players': [
                                        {
                                            delete_request: {
                                                key: {'k1': 'Veselin'}
                                            }
                                        },
                                        {
                                            delete_request: {
                                                key: {'k1': 'Anish'}
                                            }
                                        }
                                    ]
                                }
                            })

    get_item_response = client.get_item({
                                            table_name: 'chess_players',
                                            key: {
                                                'k1': 'Anish'
                                            }
                                        })
    expect(get_item_response.item).to be_nil
  end

  it 'deletes multiple items in more than one table' do
    nakamura = {
        'k1': 'Hikaru',
        'k2': 'Nakamura'
    }

    kohli = {
        'fn': 'Virat',
        'ln': 'Kohli'
    }

    client = Aws::DynamoDB::Client.new(connection_info)
    client.create_table(get_table_definition('chess_players', 'k1', 'S'))
    client.create_table(get_table_definition('cricketers', 'fn', 'S'))

    client.put_item({
                        table_name: 'chess_players',
                        item: nakamura
                    })
    client.put_item({
                        table_name: 'cricketers',
                        item: kohli
                    })

    get_item_response = client.get_item({
                                            table_name: 'chess_players',
                                            key: {
                                                'k1': 'Hikaru'
                                            }
                                        })
    expect(get_item_response.item).not_to be_nil
    expect(get_item_response.item['k1']).to eql('Hikaru')

    get_item_response = client.get_item({
                                            table_name: 'cricketers',
                                            key: {
                                                'fn': 'Virat'
                                            }
                                        })
    expect(get_item_response.item).not_to be_nil
    expect(get_item_response.item['fn']).to eql('Virat')

    client.batch_write_item({
                                request_items: {
                                    'chess_players': [
                                        {
                                            delete_request: {
                                                key: {'k1': 'Hikaru'}
                                            }
                                        }
                                    ],
                                    'cricketers': [
                                        {
                                            delete_request: {
                                                key: {'fn': 'Virat'}
                                            }
                                        }
                                    ]
                                }
                            })

    get_item_response = client.get_item({
                                            table_name: 'chess_players',
                                            key: {
                                                'k1': 'Hikaru'
                                            }
                                        })
    expect(get_item_response.item).to be_nil

    get_item_response = client.get_item({
                                            table_name: 'cricketers',
                                            key: {
                                                'fn': 'Virat'
                                            }
                                        })
    expect(get_item_response.item).to be_nil
  end

end