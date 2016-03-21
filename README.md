Tests that illustrate use of the [aws-sdk API for DynamoDB](http://docs.aws.amazon.com/sdkforruby/api/Aws/DynamoDB.html)

- [batch_get_item](spec/batch_get_item_spec.rb)
- [batch_write_item](spec/batch_write_item_spec.rb)
- [conditional_put_item](spec/conditional_put_item_spec.rb)
- [global_secondary_index](spec/global_secondary_index_spec.rb)
- [local_secondary_index](spec/local_secondary_index_spec.rb)
- [scan](spec/scan_spec.rb)
- [table_with_hash_key_and_range_key](spec/table_with_hash_key_and_range_key_spec.rb)
- [table_with_hash_key](spec/table_with_hash_key_spec.rb)
- [update_item](spec/update_item_spec.rb)

HOW-TO

- Get client-side database and server imitating DynamoDB: [dynamodb-local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html)
- Run dynamodb-local

```
# Run dynamodb-local
$ dynamodb-local --help
$ dynamodb-local &>/dev/null

# Run spec tests
$ rvm gemset use --create query
$ rvm current
ruby-2.2.1@query
$ bundle install --binstubs
$ rspec
```