require 'aws-sdk'

RSpec.describe 'global secondary indexes' do

  it 'use a global secondary index to locate items when the hash key values are not known', :focus => true do

    instance_states = [
        {
            'instance_id': 'i-12345abc',
            'at_time': '2016-03-29T12:01:23+05:30',
            'instance_state': 'pending',
            'Name': 'api-1'
        },
        {
            'instance_id': 'i-87999def',
            'at_time': '2016-03-29T12:02:37+05:30',
            'instance_state': 'running',
            'Name': 'api-2'
        },
        {
            'instance_id': 'i-12345abc',
            'at_time': '2016-03-29T12:01:23+05:30',
            'instance_state': 'running',
            'Name': 'api-1'
        },
        {
            'instance_id': 'i-12345abc',
            'at_time': '2016-03-29T17:01:23+05:30',
            'instance_state': 'stopping',
            'Name': 'api-1'
        },
        {
            'instance_id': 'i-12345abc',
            'at_time': '2016-03-29T18:01:23+05:30',
            'instance_state': 'rebooting',
            'Name': 'api-1'
        },
        {
            'instance_id': 'i-12345abc',
            'at_time': '2016-03-29T19:01:23+05:30',
            'instance_state': 'shutting-down',
            'Name': 'api-1'
        },
        {
            'instance_id': 'i-12345abc',
            'at_time': '2016-03-29T19:01:23+05:30',
            'instance_state': 'terminated',
            'Name': 'api-1'
        },
        {
            'instance_id': 'i-87654def',
            'at_time': '2016-03-30T03:42:19+05:30',
            'instance_state': 'terminated',
            'Name': 'api-2'
        },
    ]

    client = Aws::DynamoDB::Client.new(connection_info)
    delete_table('instance_states')
    client.create_table({
                            table_name: 'instance_states',
                            attribute_definitions: [
                                {
                                    attribute_name: 'instance_id',
                                    attribute_type: 'S'
                                },
                                {
                                    attribute_name: 'at_time',
                                    attribute_type: 'S'
                                },
                                {
                                    attribute_name: 'instance_state',
                                    attribute_type: 'S'
                                }
                            ],
                            key_schema: [
                                {
                                    attribute_name: 'instance_id',
                                    key_type: 'HASH'
                                },
                                {
                                    attribute_name: 'at_time',
                                    key_type: 'RANGE'
                                }
                            ],
                            global_secondary_indexes: [
                                {
                                    index_name: 'gsi_instance_state',
                                    key_schema: [
                                        {
                                            attribute_name: 'instance_state',
                                            key_type: 'HASH'
                                        },
                                        {
                                            attribute_name: 'at_time',
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

    instance_states.each do |state|
      client.put_item({
                          table_name: 'instance_states',
                          item: state
                      })
    end

    #Retrieve instance state information for instances in a certain state
    response = client.query({
                                table_name: 'instance_states',
                                index_name: 'gsi_instance_state',
                                key_condition_expression: 'instance_state = :v_state AND at_time >= :v_at_time',
                                expression_attribute_values: {
                                    ':v_state': 'running',
                                    ':v_at_time': '2016-03-29T12:01:23+05:30'
                                }
                            })

    running_instance_states = response.items
    expect(running_instance_states).not_to be_empty
    running_instances = response.items.collect do |instance_state|
      instance_state['instance_id']
    end
    expect(running_instances.uniq).to match_array(['i-12345abc', 'i-87999def'])

    #TODO: Retrieve the most recent state for an instance
  end

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
