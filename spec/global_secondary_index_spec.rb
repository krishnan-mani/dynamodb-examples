require 'aws-sdk'

RSpec.describe 'global secondary indexes' do

  it 'create and query a global secondary index' do
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
        },
        corus_2015: {
            'tournament': 'Corus 2015',
            'finish': 'first place',
            'first_name': 'Magnus',
            'last_name': 'Carlsen'
        },
        hoogovens_1993: {
            'tournament': 'Hoogovens 1993',
            'finish': 'first_place',
            'first_name': 'Anatoly',
            'last_name': 'Karpov'
        }
    }

    client = Aws::DynamoDB::Client.new(connection_info)
    delete_table('tournaments')
    client.create_table({
                            table_name: 'tournaments',
                            attribute_definitions: [
                                {
                                    attribute_name: 'tournament',
                                    attribute_type: 'S'
                                },
                                {
                                    attribute_name: 'finish',
                                    attribute_type: 'S'
                                },
                                {
                                    attribute_name: 'first_name',
                                    attribute_type: 'S'
                                },
                                {
                                    attribute_name: 'last_name',
                                    attribute_type: 'S'
                                }
                            ],
                            key_schema: [
                                {
                                    attribute_name: 'tournament',
                                    key_type: 'HASH'
                                },
                                {
                                    attribute_name: 'finish',
                                    key_type: 'RANGE'
                                }
                            ],
                            global_secondary_indexes: [
                                {
                                    index_name: 'gsi_name',
                                    key_schema: [
                                        {
                                            attribute_name: 'first_name',
                                            key_type: 'HASH'
                                        },
                                        {
                                            attribute_name: 'last_name',
                                            key_type: 'RANGE'
                                        }
                                    ],
                                    projection: {
                                        projection_type: 'ALL'
                                    },
                                    provisioned_throughput: {
                                        read_capacity_units: 1,
                                        write_capacity_units: 1
                                    }
                                }

                            ],
                            provisioned_throughput: {
                                read_capacity_units: 1,
                                write_capacity_units: 1
                            }
                        })

    tournament_wins.each_value do |win|
      client.put_item({
                          table_name: 'tournaments',
                          item: win
                      })
    end

    karpov_wins_response = client.query({
                                           table_name: 'tournaments',
                                           index_name: 'gsi_name',
                                           key_condition_expression: 'first_name = :v_fn AND last_name = :v_ln',
                                           expression_attribute_values: {
                                               ':v_fn': 'Anatoly',
                                               ':v_ln': 'Karpov'
                                           }
                                       })
    expect(karpov_wins_response.items).not_to be_empty
    expect(karpov_wins_response.items.first['tournament']).to eql('Hoogovens 1993')
  end

end