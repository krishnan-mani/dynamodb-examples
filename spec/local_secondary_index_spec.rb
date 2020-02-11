require 'aws-sdk-dynamodb'

RSpec.describe 'local secondary indexes' do

  it 'query using an attribute other than the sort key on the table' do
    delete_table('wins')
    client = Aws::DynamoDB::Client.new(connection_info)

    client.create_table({
                            table_name: 'wins',
                            attribute_definitions: [
                                {
                                    attribute_name: 'finish',
                                    attribute_type: 'S'
                                },
                                {
                                    attribute_name: 'tournament',
                                    attribute_type: 'S'
                                },
                                {
                                    attribute_name: 'first_name',
                                    attribute_type: 'S'
                                }
                            ],
                            key_schema: [
                                {
                                    attribute_name: 'finish',
                                    key_type: 'HASH'
                                },
                                {
                                    attribute_name: 'tournament',
                                    key_type: 'RANGE'
                                }
                            ],
                            local_secondary_indexes: [
                                {
                                    index_name: 'lsi_first_name',
                                    key_schema: [
                                        {
                                            attribute_name: 'finish',
                                            key_type: 'HASH'
                                        },
                                        {
                                            attribute_name: 'first_name',
                                            key_type: 'RANGE'
                                        }
                                    ],
                                    projection: {
                                        projection_type: 'ALL'
                                    }
                                }
                            ],
                            provisioned_throughput: {
                                read_capacity_units: 1,
                                write_capacity_units: 1
                            }
                        })
    tournament_wins = {
        reggio_emilia_93: {
            'tournament': 'Reggio Emilia 1993',
            'finish': 'first place',
            'first_name': 'Vishy',
            'last_name': 'Anand'
        },
        tilburg_92: {
            'tournament': 'Tilburg 1992',
            'finish': 'first place',
            'category': 18,
            'first_name': 'Vishy',
            'last_name': 'Anand',
        },
        world_championship_2007: {
            'tournament': 'WC 2007',
            'finish': 'first place',
            'first_name': 'Vishy',
            'last_name': 'Anand'
        },
        world_championship_2008: {
            'tournament': 'WC 2008',
            'finish': 'first place',
            'first_name': 'Vishy',
            'last_name': 'Anand'
        },
        stavanger_2015: {
            'tournament': 'Stavanger 2015',
            'finish': 'second place',
            'first_name': 'Vishy',
            'last_name': 'Anand'
        }
    }

    tournament_wins.each_value do |win|
      client.put_item({
                          table_name: 'wins',
                          item: win
                      })
    end

    vishy_second_places_response = client.query({
                                                    table_name: 'wins',
                                                    index_name: 'lsi_first_name',
                                                    key_condition_expression: '#finish = :v_finish AND first_name = :v_fn',
                                                    expression_attribute_names: {
                                                        '#finish': 'finish'
                                                    },
                                                    expression_attribute_values: {
                                                        ':v_finish': 'second place',
                                                        ':v_fn': 'Vishy'
                                                    }
                                                })

    expect(vishy_second_places_response).not_to be_nil
    expect(vishy_second_places_response.items.first['tournament']).to eql('Stavanger 2015')
  end

end