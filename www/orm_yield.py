import asyncio
import aiomysql
import sys
import logging; logging.basicConfig(level=logging.INFO)
from logging import log

def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('creat database connection pool...')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host = kw.get('host', '127.0.0.1'),
        port = kw.get('port', 3306),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        charset = kw.get('charset', 'utf8'),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
        loop = loop
    )

@asyncio.coroutine
def destory_pool():
    global __pool
    if __pool is not None :
        __pool.close()
        yield from __pool.wait_closed()

@asyncio.coroutine
def select(sql, args=None, size=None):
    print('select : ', sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned : %s' % len(rs))
        conn.close()
        return rs

@asyncio.coroutine
def execute(sql, args):
    print('execute : ', sql, args)
    global __pool
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '%s'), args)
            yield from conn.commit()
            affected = cur.rowcount
            yield from cur.close()
            print('execute : ', affected)
        except BaseException as e:
            raise RuntimeError(r"MYSQL have same date %s" % args)
        conn.close()
        return affected


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='bigint'):
        super().__init__(name, ddl, primary_key, default)


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        #排除Model类本身
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)

        #获取表的名字
        tableName = attrs.get('__table__', None) or name
        logging.info('found model:%s (tables:%s)' % (name, tableName))

        #获取所有Filed和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        #遍历类的所有属性，把数据库表单 另外存储
        for k, v in attrs.items():  # 获取class的所有属性
            if isinstance(v, Field):    #判断类型 是不是数据库数据类型
                logging.info('  found mapping:%s == > %s' % (k, v))
                mappings[k] = v     #存入mappings中
                if v.primary_key:   #这个数据是主键
                    #找到主键
                    if primaryKey:  #判断是否为单一主键, 不是就抛异常
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)    #除主键以外的属性名
        if not primaryKey:  #没有主键抛异常
            raise RuntimeError('Primary key not found')

        #把数据库类别从属性中抛掉
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fileds = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings #保存成员的数据类型的映射关系
        attrs['__talbe__'] = tableName
        attrs['__primary_key__'] = primaryKey #主键属性名
        attrs['__fields__'] = fields #除主键以外的属性名

        #构造默认的SELECT, INSERT, UPDATE, DELETE
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, \
                                        ', '.join(escaped_fileds), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' %\
                              (tableName, ', '.join(escaped_fileds), primaryKey, \
                               create_args_string(len(escaped_fileds)+1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % \
                              (tableName, ', '.join(map(lambda f:'`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mapings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s:%s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        print('save---', args)
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

    @classmethod
    @asyncio.coroutine
    def find(cls, pk):
        'find object by primary key'
        rs = yield from select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @classmethod
    @asyncio.coroutine
    def findAll(cls, **kw):
        rs = []
        if len(kw) == 0:
            rs = yield from select(cls.__select__, None)
        else:
            args=[]
            values=[]
            for k, v in kw.items():
                args.append('%s=?' % k )
                values.append(v)
            rs = yield from select('%s where %s ' % (cls.__select__,  ' and '.join(args)), values)
        return rs

    @classmethod
    @asyncio.coroutine
    def update(cls, **kw):
        ret = 0
        args = []
        values=[]
        primary = kw.get(cls.__primary_key__)
        kw.pop(cls.__primary_key__)
        print(122, kw)
        for k, v in kw.items():
            values.append(v)
        values.append(primary)
        print('update', cls.__update__, kw.get(cls.__primary_key__))
        ret = yield from execute('%s' % (cls.__update__), values)
        return ret

    @classmethod
    @asyncio.coroutine
    def remove(cls, **kw):
        primaryKey = kw.get(cls.__primary_key__)
        ret = yield from execute('%s' % (cls.__delete__), primaryKey)
        return ret


class User(Model):
    __table__ = 'users'

    id = IntegerField(primary_key = True)
    name = StringField()

    def show(self):
        print(1, '__mappings__:', self.__mappings__)
        print(2, '__table__:', self.__table__)
        print(3, '__primary_key__:', self.__primary_key__)
        print(4, '__fields__:', self.__fields__)
        print(5, '__select__:', self.__select__)
        print(6, '__insert__:', self.__insert__)
        print(7, '__update__:', self.__update__)
        print(8, '__delete__:', self.__delete__)

"""
user = User(id=123, name='Michael', job='engneer')
print('-------create finish-----------')
user.show()
print(9, user)
"""


#创建异步事件的句柄
loop = asyncio.get_event_loop()

#创建实例
@asyncio.coroutine
def test():
    yield from create_pool(loop=loop,host='localhost', port=3306, user='root', password='1234', db='User')
    #user = User(id=10, name='Ablin')
    #yield from user.save()
    #r = yield from User.find('10')
    #print(r)
    r = yield from User.findAll()
    print(1, r)
    r = yield from User.findAll(id='12')
    print(2, r)
    r = yield from User.findAll(name='jw', id='10')
    print(3, r)

    #r = yield from User.update(name='Abl2n', id='10')
    # r = yield from User.remove(id='10')

    # yield from destory_pool()

loop.run_until_complete(test())
loop.close()
if loop.is_closed():
    sys.exit(0)