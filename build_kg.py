#!/usr/bin/env python3
# coding: utf-8
# File: build_kg.py
# Author: lhy<lhy_in_blcu@126.com,https://huangyong.github.io>
# Date: 19-3-31

import json
import os
from py2neo import Graph

PASSWORD = 'paint-pixel-sulfur-paint-season-4270'

class GoodsKg:
    def __init__(self):
        cur = os.path.dirname(os.path.abspath(__file__))
        self.data_path = os.path.join(cur, 'data/goods_info.json')

        # ✅ 使用新版 py2neo 推荐连接方式
        self.g = Graph("bolt://localhost:7687", auth=("neo4j", PASSWORD))

    '''读取数据'''
    def read_data(self):
        rels_goods = []
        rels_brand = []
        goods_attrdict = {}
        concept_goods = set()
        concept_brand = set()
        count = 0

        with open(self.data_path, encoding='utf-8') as f:
            for line in f:
                count += 1
                print(count)
                line = line.strip()
                data = json.loads(line)
                first_class = data['fisrt_class'].replace("'", '')
                second_class = data['second_class'].replace("'", '')
                third_class = data['third_class'].replace("'", '')
                attr = data['attrs']

                concept_goods.update([first_class, second_class, third_class])
                rels_goods.append(f'{second_class}@is_a@属于@{first_class}')
                rels_goods.append(f'{third_class}@is_a@属于@{second_class}')

                if attr and '品牌' in attr:
                    brands = attr['品牌'].split(';')
                    for brand in brands:
                        brand = brand.replace("'", '')
                        concept_brand.add(brand)
                        rels_brand.append(f'{brand}@sales@销售@{third_class}')

                goods_attrdict[third_class] = {
                    name: value for name, value in attr.items() if name != '品牌'
                }

        return concept_brand, concept_goods, rels_goods, rels_brand

    '''构建图谱'''
    def create_graph(self):
        concept_brand, concept_goods, rels_goods, rels_brand = self.read_data()
        print('creating nodes....')
        self.create_node('Product', concept_goods)
        self.create_node('Brand', concept_brand)
        print('creating edges....')
        self.create_edges(rels_goods, 'Product', 'Product')
        self.create_edges(rels_brand, 'Brand', 'Product')

    '''批量建立节点'''
    def create_node(self, label, nodes):
        pairs = []
        bulk_size = 1000
        batch = 0
        total = len(nodes)
        print(f'Total {total} nodes to create...')

        for i, node_name in enumerate(nodes, 1):
            safe_name = node_name.replace('"', '')  # 防止引号错误
            sql = f"""CREATE(:{label} {{name: "{safe_name}"}})"""
            pairs.append(sql)

            if i % bulk_size == 0 or i == total:
                try:
                    self.g.run('\n'.join(pairs))
                except Exception as e:
                    print(f"Error in batch {batch}: {e}")
                print(f'{i} / {total} nodes created.')
                pairs = []
                batch += 1

    '''构造图谱关系边'''
    def create_edges(self, rels, start_type, end_type):
        count = 0
        for rel in set(rels):
            count += 1
            parts = rel.split('@')
            if len(parts) != 4:
                continue
            start_name, rel_type, rel_name, end_name = parts
            start_name = start_name.replace('"', '')
            end_name = end_name.replace('"', '')
            rel_name = rel_name.replace('"', '')
            try:
                sql = (
                    f'MATCH (m:{start_type} {{name: "{start_name}"}}), '
                    f'(n:{end_type} {{name: "{end_name}"}}) '
                    f'CREATE (m)-[:{rel_type} {{name: "{rel_name}"}}]->(n)'
                )
                self.g.run(sql)
            except Exception as e:
                print(f"[Edge Error] {e}")
            if count % 10 == 0:
                print(f'{count} edges created...')

if __name__ == '__main__':
    handler = GoodsKg()
    handler.create_graph()
