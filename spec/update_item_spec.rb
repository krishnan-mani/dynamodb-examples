require 'aws-sdk'

RSpec.describe 'update_item' do

  it 'edits an item in-place' do
    young_anand = {
        'name': 'Vishy',
        'speed': 'lightning fast'
    }

    recreate_table('chess_players', 'name', 'S')
    client = Aws::DynamoDB::Client.new(connection_info)
    client.put_item({
                        table_name: 'chess_players',
                        item: young_anand
                    })

    response = client.get_item({
                        table_name: 'chess_players',
                        key: {
                            'name': 'Vishy'
                        }
                    })
    expect(response.item['speed']).to eql('lightning fast')

    client.update_item({
                           table_name: 'chess_players',
                           key: {
                               'name': 'Vishy'
                           },
                           update_expression: "SET speed = :v_speed",
                           expression_attribute_values: {
                               ':v_speed': 'sedate'
                           }
                       })

    new_response = client.get_item({
                                   table_name: 'chess_players',
                                   key: {
                                       'name': 'Vishy'
                                   }
                               })
    expect(new_response.item['speed']).to eql('sedate')
  end

end