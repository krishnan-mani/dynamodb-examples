Tests (in Ruby) to illustrate use of the [aws-sdk API for DynamoDB](http://docs.aws.amazon.com/sdkforruby/api/Aws/DynamoDB.html)

- Operations such as `list_tables`, `create_table`, `delete_table`, `get_item`, and `query` are included in most spec tests
- [table with hash key and range key](spec/table_with_hash_key_and_range_key_spec.rb)
- [table with hash key](spec/table_with_hash_key_spec.rb)
- [global secondary indexes](spec/global_secondary_index_spec.rb)
- [local secondary indexes](spec/local_secondary_index_spec.rb)
- [update_item](spec/update_item_spec.rb)
- [conditional put_item](spec/conditional_put_item_spec.rb)
- [batch_get_item](spec/batch_get_item_spec.rb)
- [batch_write_item](spec/batch_write_item_spec.rb)
- [scan](spec/scan_spec.rb)

#### HOW-TO

- Get local database imitating DynamoDB: [dynamodb-local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html)
- Run `dynamodb-local`
- Run `rspec` tests

```
# Run dynamodb-local
$ dynamodb-local --help
$ dynamodb-local &

# Run tests

$ rvm gemset use --create dynamodb-examples
$ rvm current
ruby-2.6.0@dynamodb-examples
$ bundle install --binstubs
$ rspec -f d

```