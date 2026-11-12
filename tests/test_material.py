"""已有资料摘要与基本配置检查。"""
import hashlib,json,tempfile,unittest
from pathlib import Path
from warehouse_data.io import load,asset,integer,unique
from warehouse_data.domain import summarize

class MaterialTests(unittest.TestCase):
    def test_normal(self):
        v,h=load('data/normal.json');s=summarize(v,Path('data'))
        self.assertEqual(len(h),64);self.assertTrue(s);self.assertEqual(s,summarize(json.loads(json.dumps(v)),Path('data')))
    def test_invalid(self):
        with self.assertRaises((ValueError,KeyError,TypeError)):summarize(load('data/invalid-config.json')[0],Path('data'))
    def test_boundary_materials(self):
        for p in Path('data').glob('*.json'):
            if p.stem=='invalid-config':continue
            with self.subTest(file=p.name):self.assertIsInstance(summarize(load(p)[0],p.parent),dict)
    def test_numeric(self):
        for x in (True,-1,1.2):
            with self.assertRaises(ValueError):integer(x)
        self.assertEqual(integer(3,3),3)
    def test_identity_and_path(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'x';p.write_bytes(b'abc');ref={'path':'x','bytes':3,'sha256':hashlib.sha256(b'abc').hexdigest()};self.assertEqual(asset(d,ref),b'abc')
            for r in ({**ref,'path':'../outside'},{**ref,'sha256':'wrong'},{**ref,'bytes':4}):
                with self.assertRaises(ValueError):asset(d,r)
        with self.assertRaises(ValueError):unique([{'id':'a'},{'id':'a'}],'id')
    def test_json_shape(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'资料.json'
            for x in ('[]','null','{"n":Infinity}'):
                p.write_text(x,encoding='utf8')
                with self.assertRaises(ValueError):load(p)
