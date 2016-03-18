require 'aws-sdk'

RSpec.describe 'scan' do

  it 'scan with limit' do
    harikrishna = {
        'k1': 'Pentala',
        'k2': 'Harikrishna'
    }
    sasikiran = {
        'k1': 'Krishnan',
        'k2': 'Sasikiran'
    }
    adhiban = {
        'k1': 'Bhaskaran',
        'k2': 'Adhiban'
    }
    adhiban_doppelganger = {
        'k1': 'Bhaskaran',
        'k2': 'Anbazhagan'
    }

    recreate_table('chess_players', 'k1', 'S', 'k2', 'S')

    client = Aws::DynamoDB::Client.new(connection_info)
    client.put_item({
                        table_name: 'chess_players',
                        item: harikrishna
                    })
    client.put_item({
                        table_name: 'chess_players',
                        item: sasikiran
                    })
    client.put_item({
                        table_name: 'chess_players',
                        item: adhiban
                    })
    client.put_item({
                        table_name: 'chess_players',
                        item: adhiban_doppelganger
                    })

    get_item_response = client.get_item({
                                            table_name: 'chess_players',
                                            key: {
                                                'k1': 'Pentala',
                                                'k2': 'Harikrishna'
                                            }
                                        })
    expect(get_item_response.item).not_to be_nil
    expect(get_item_response.item['k2']).to eql('Harikrishna')

    scan_response = client.scan({
                                    table_name: 'chess_players',
                                    limit: 2
                                })

    expect(scan_response.scanned_count).to eql(2)
    expect(scan_response.count).to eql(2)
    expect(scan_response.last_evaluated_key).not_to be_nil

    next_scan_response = client.scan({
                                         table_name: 'chess_players',
                                         exclusive_start_key: scan_response.last_evaluated_key,
                                         limit: 1
                                     })
    expect(next_scan_response.scanned_count).to eql(1)
    expect(next_scan_response.count).to eql(1)
    expect(next_scan_response.items.first['k2']).to eql('Adhiban')

    last_scan_response = client.scan({
                                         table_name: 'chess_players',
                                         exclusive_start_key: next_scan_response.last_evaluated_key
                                     })
    expect(last_scan_response.last_evaluated_key).to be_nil
    expect(last_scan_response.count).to eql(1)
    expect(last_scan_response.items.first['k2']).to eql('Anbazhagan')
  end

end