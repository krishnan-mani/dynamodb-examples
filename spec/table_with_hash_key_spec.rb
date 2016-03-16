require_relative '../lib/writer'

require 'aws-sdk'

RSpec.describe Writer do
  connection_info = {:region => 'us-east-1', :endpoint => 'http://localhost:8000'}

  before(:each) do
    client = Aws::DynamoDB::Client.new(connection_info)
    tables = client.list_tables.table_names
    tables.each do |table|
      client.delete_table({
                              table_name: table
                          })
    end
  end

  it 'saves a rich item by hash key to DynamoDB' do
    item = {
        k1: 'v1',
        k2: 23,
        k3: true,
        k4: [1, 2, 3],
        k5: {
            'k51': 'abc',
            'k52': 'def'
        }
    }
    Writer.new(connection_info).save_foo(item)

    client = Aws::DynamoDB::Client.new(connection_info)
    response = client.get_item({
                                   table_name: 'foo',
                                   key: {
                                       k1: 'v1'
                                   }

                               })

    expect(response.item[:k1]).to eql(item['k1'])
    expect(response.item[:k2]).to eql(item['k2'])
  end

  it 'saves disparate items by hash key to DynamoDB' do
    items = [{k1: 'x1', k2: [1, 2, 3]}, {k1: 'x2', k2: true}]
    Writer.new(connection_info).save_foo(*items)

    client = Aws::DynamoDB::Client.new(connection_info)
    response = client.get_item({
                                   table_name: 'foo',
                                   key: {
                                       k1: 'x2'
                                   }
                               })

    expect(response.item['k2']).to be_truthy

    response = client.get_item({
                                   table_name: 'foo',
                                   key: {
                                       k1: 'x1'
                                   }
                               })

    expect(response.item['k2'].length).to eql(3)
  end

  it 'supports querying items in DynamoDB to obtain a count' do
    items = [{k1: 'a1', k2: 12, k3: 'abc'}, {k1: 'a1', k2: 24, k3: 'def'}]
    Writer.new(connection_info).save_xyz(*items)

    client = Aws::DynamoDB::Client.new(connection_info)
    response = client.query({
                                table_name: 'xyz',
                                select: 'COUNT',
                                key_condition_expression: 'k1 = :v_k1',
                                expression_attribute_values: {
                                    ':v_k1': 'a1'
                                }
                            })

    expect(response.count).to eql(2)
  end

  it 'supports querying using key condition expression' do
    item = {
        'a866': 206,
        Title: "20-Bicycle 206",
        Description: "206 description",
        BicycleType: "Hybrid",
        Brand: "Brand-Company C",
        Price: 500,
        Color: ["Red", "Black"],
        ProductCategory: "Bike",
        InStock: true,
        QuantityOnHand: nil,
        RelatedItems: [
            341,
            472,
            649
        ],
        Pictures: {
            FrontView: "http://example.com/products/206_front.jpg",
            RearView: "http://example.com/products/206_rear.jpg",
            SideView: "http://example.com/products/206_left_side.jpg"
        },
        ProductReviews: {
            FiveStar: [
                "Excellent! Can't recommend it highly enough!  Buy it!",
                "Do yourself a favor and buy this."
            ],
            OneStar: [
                "Terrible product!  Do not buy this."
            ]
        }
    }
    Writer.new(connection_info).save_product(item)

    client = Aws::DynamoDB::Client.new(connection_info)
    response = client.query({
                                table_name: 'products',
                                key_condition_expression: "a866 = :v_id",
                                expression_attribute_values: {
                                    ':v_id': 206
                                }
                            })

    expect(response.items[0]).not_to be_nil
    found_item = response.items[0]
    expect(found_item['Title']).to eql("20-Bicycle 206")
  end

end
