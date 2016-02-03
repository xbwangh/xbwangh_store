from keystone.common import sql
from keystone.common import utils
from keystone import config
from keystone import exception
from keystone.i18n import _
from keystone import identity
from keystone import hello
from keystone import token
from pprint import pprint
from keystone.assignment.backends.sql import RoleAssignment
# Import assignment sql to ensure that the models defined in there are
# available for the reference from User and Group to Domain.id.
from keystone.assignment.backends import sql as assignment_sql  # noqa
from keystone.openstack.common import log
import six

LOG = log.getLogger(__name__)
CONF = config.CONF

class Test(sql.ModelBase,sql.ModelDictMixin):
    __tablename__='test'
    attributes = ['id', 'user_num', 'user_name']
    id=sql.Column(sql.String(64), primary_key=True)
    user_num=sql.Column(sql.String(10))
    user_name=sql.Column(sql.String(40))

class Test_DES(sql.ModelBase,sql.ModelDictMixin):
    __tablename__='test_des'
    attributes = ['id', 'user_num', 'user_description']
    id=sql.Column(sql.String(10), primary_key=True)
    user_num=sql.Column(sql.String(10),sql.ForeignKey('test.user_num'))
    user_description=sql.Column(sql.String(255))

class TestKeyStone(hello.Driver):

    def __init__(self, conf=None):
        super(TestKeyStone, self).__init__()

    @sql.truncated
    def list_hellos(self, hints):
        session = sql.get_session()
        query = session.query(Test)
        user_refs = sql.filter_limit_query(Test, query, hints)
        return [x.to_dict() for x in user_refs.all()]

    @sql.truncated
    def get_hello(self, hello_id):
        session = sql.get_session()
        hello_ref = session.query(Test).get(hello_id)
        if not hello_ref:
            raise exception.UserNotFound(hello_id=hello_id, biz_code='sc201')
        return hello_ref

    def get_hello(self, hello_id):
        session = sql.get_session()
        hello_ref =session.query(Test).join(Test_DES)
        print(hello_ref.all())
        return self._get_hello(session, hello_id).to_dict()

    def _get_hello(self, session, hello_id):
        ref = session.query(Test).get(hello_id)
        if not ref:
            raise exception.ServiceNotFound(hello_id=hello_id, biz_code='sc601')
        return ref

    def delete_hello(self, hello_id):
        session = sql.get_session()
        with session.begin():
            ref = self._get_hello(session, hello_id)
            session.delete(ref)

    def update_hello(self, hello_id, test):
        session = sql.get_session()
        with session.begin():
            ref= self._get_hello(session, hello_id)
            old_test_dict= ref.to_dict()
            old_test_dict.update(test)
            new_test= Test.from_dict(old_test_dict)
            for attr in Test.attributes:
                if attr != 'id' and attr in test:
                    setattr(ref, attr, getattr(new_test, attr))
        return old_test_dict

    def create_hello(self, hello_id, hello):
        session = sql.get_session()
        with session.begin():
            hello_ref = Test.from_dict(hello)
            session.add(hello_ref)
        return hello_ref.to_dict()
