import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

r.set('foo', 'bar')
# True
value = r.get('foo')
print(value)
# bar

r.hset('user-session:123', mapping={
    'name': 'John',
    "surname": 'Smith',
    "company": 'Redis',
    "age": 29
})
# True

value = r.hgetall('user-session:123')
print(value)
# {'surname': 'Smith', 'name': 'John', 'company': 'Redis', 'age': '29'}

r.close()