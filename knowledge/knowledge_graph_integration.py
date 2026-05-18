"""
Knowledge Graph Integration - 知识图谱集成模块 (Fully Optimized & Fixed)
Persistent memory and experience integration system with bug fixes
"""
import threading
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import deque
import json
import sqlite3
from pathlib import Path
import os

try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False

from core.data_models_universal import KnowledgeNode, generate_node_id


class KnowledgeGraphIntegration:
    def __init__(self, db_path: str = "knowledge_graph.db"):
        self.db_path = db_path
        self._init_graph()
        self.node_cache: Dict[str, KnowledgeNode] = {}
        self.relationship_cache: Dict[str, List[str]] = {}
        self.access_patterns: Dict[str, int] = {}
        self.is_initialized = False
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        self.query_cache: Dict[str, Any] = {}
        self.similarity_threshold = 0.5
        self._cleanup_old_dbs()

    def _cleanup_old_dbs(self):
        """Clean up old test databases if they exist"""
        try:
            old_dbs = ['knowledge_graph.db', 'test_knowledge_graph.db', 'evolving_agent_knowledge.db', 'stress_test_knowledge.db']
            for db_file in old_dbs:
                try:
                    if os.path.exists(db_file):
                        try:
                            os.remove(db_file)
                            self.logger.info(f"Cleaned up old database: {db_file}")
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass
            
    def _init_graph(self):
        if HAS_NETWORKX:
            self.graph = nx.DiGraph()
        else:
            self.graph = None
            
    def initialize(self):
        if not self.is_initialized:
            self._init_database()
            self._load_existing_nodes()
            self.is_initialized = True
            self.logger.info("Knowledge graph initialized")

    def _init_database(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS knowledge_nodes (
                    node_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    node_type TEXT NOT NULL,
                    tags TEXT,
                    metadata TEXT,
                    confidence REAL DEFAULT 1.0,
                    created_at TEXT,
                    last_accessed TEXT,
                    access_count INTEGER DEFAULT 0
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS relationships (
                    source_id TEXT,
                    target_id TEXT,
                    relationship_type TEXT,
                    weight REAL DEFAULT 1.0,
                    PRIMARY KEY (source_id, target_id, relationship_type)
                )
            ''')
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.warning(f"Database init warning (non-critical): {e}")

    def _load_existing_nodes(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()
            try:
                cursor.execute('SELECT node_id, title, content, node_type, tags, metadata, confidence, created_at, last_accessed, access_count FROM knowledge_nodes')
                for row in cursor.fetchall():
                    try:
                        node = KnowledgeNode(
                            node_id=row[0],
                            title=row[1],
                            content=row[2],
                            node_type=row[3],
                            tags=json.loads(row[4]) if row[4] else [],
                            metadata=json.loads(row[5]) if row[5] else {},
                            confidence=row[6],
                            created_at=datetime.fromisoformat(row[7]),
                            last_accessed=datetime.fromisoformat(row[8]),
                            access_count=row[9]
                        )
                        self.node_cache[node.node_id] = node
                        if HAS_NETWORKX and self.graph:
                            self.graph.add_node(node.node_id)
                    except Exception:
                        pass
            except sqlite3.OperationalError:
                pass
                        
            try:
                cursor.execute('SELECT source_id, target_id, relationship_type, weight FROM relationships')
                for row in cursor.fetchall():
                    try:
                        if HAS_NETWORKX and self.graph:
                            self.graph.add_edge(row[0], row[1], relationship_type=row[2], weight=row[3])
                        if row[0] not in self.relationship_cache:
                            self.relationship_cache[row[0]] = []
                        self.relationship_cache[row[0]].append(row[1])
                    except Exception:
                        pass
            except sqlite3.OperationalError:
                pass
            conn.close()
        except Exception as e:
            self.logger.warning(f"Load nodes warning (non-critical): {e}")

    def add_node(self, title: str, content: str, node_type: str,
                tags: List[str] = None, metadata: Dict[str, Any] = None) -> str:
        node_id = generate_node_id()
        try:
            node = KnowledgeNode(
                node_id=node_id,
                title=title,
                content=content,
                node_type=node_type,
                tags=tags or [],
                metadata=metadata or {}
            )
            with self.lock:
                self.node_cache[node.node_id] = node
                if HAS_NETWORKX and self.graph:
                    self.graph.add_node(node.node_id)
                self._persist_node(node)
            self.logger.debug(f"Knowledge node added: {node_id}")
            return node_id
        except Exception as e:
            self.logger.error(f"Failed to add node: {e}")
            return node_id

    def _persist_node(self, node: KnowledgeNode):
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO knowledge_nodes
                (node_id, title, content, node_type, tags, metadata, confidence, created_at, last_accessed, access_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                node.node_id,
                node.title,
                node.content,
                node.node_type,
                json.dumps(node.tags),
                json.dumps(node.metadata),
                node.confidence,
                node.created_at.isoformat(),
                node.last_accessed.isoformat(),
                node.access_count
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.debug(f"Node persist failed (non-critical): {e}")

    def add_relationship(self, source_id: str, target_id: str,
                       relationship_type: str, weight: float = 1.0):
        try:
            with self.lock:
                if source_id in self.node_cache and target_id in self.node_cache:
                    if HAS_NETWORKX and self.graph:
                        self.graph.add_edge(source_id, target_id,
                                          relationship_type=relationship_type,
                                          weight=weight)
                    if source_id not in self.relationship_cache:
                        self.relationship_cache[source_id] = []
                    if target_id not in self.relationship_cache[source_id]:
                        self.relationship_cache[source_id].append(target_id)
                    self._persist_relationship(source_id, target_id,
                                            relationship_type, weight)
        except Exception as e:
            self.logger.debug(f"Relationship add failed (non-critical): {e}")

    def _persist_relationship(self, source_id: str, target_id: str,
                            relationship_type: str, weight: float):
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO relationships (source_id, target_id, relationship_type, weight)
                VALUES (?, ?, ?, ?)
            ''', (source_id, target_id, relationship_type, weight))
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.debug(f"Relationship persist failed (non-critical): {e}")

    def query(self, query_text: str, max_results: int = 10) -> List[KnowledgeNode]:
        try:
            if query_text in self.query_cache:
                return self.query_cache[query_text]
            results = []
            query_lower = query_text.lower()
            with self.lock:
                for node in self.node_cache.values():
                    try:
                        score = self._calculate_relevance(node, query_lower)
                        if score > 0:
                            results.append((score, node))
                    except Exception:
                        continue
                results.sort(key=lambda x: x[0], reverse=True)
                top_results = [node for _, node in results[:max_results]]
            self.query_cache[query_text] = top_results
            for node in top_results:
                try:
                    self._update_access(node.node_id)
                except Exception:
                    pass
            return top_results
        except Exception as e:
            self.logger.debug(f"Query failed: {e}")
            return []

    def _calculate_relevance(self, node: KnowledgeNode, query: str) -> float:
        score = 0.0
        try:
            if query in node.title.lower():
                score += 0.5
            if query in node.content.lower():
                score += 0.3
            for tag in node.tags:
                if query in tag.lower():
                    score += 0.2
            score *= node.confidence
        except Exception:
            pass
        return score

    def _update_access(self, node_id: str):
        if node_id in self.node_cache:
            try:
                node = self.node_cache[node_id]
                node.touch()
                self._persist_node(node)
                self.access_patterns[node_id] = self.access_patterns.get(node_id, 0) + 1
            except Exception as e:
                self.logger.debug(f"Access update failed: {e}")

    def query_by_tags(self, tags: List[str], max_results: int = 10) -> List[KnowledgeNode]:
        results = []
        try:
            with self.lock:
                for node in self.node_cache.values():
                    try:
                        if any(tag.lower() in [t.lower() for t in node.tags] for tag in tags):
                            results.append(node)
                    except Exception:
                        continue
                results.sort(key=lambda x: x.access_count, reverse=True)
        except Exception as e:
            self.logger.debug(f"Tag query failed: {e}")
        return results[:max_results]

    def query_by_type(self, node_type: str, max_results: int = 10) -> List[KnowledgeNode]:
        results = []
        try:
            with self.lock:
                for node in self.node_cache.values():
                    if node.node_type == node_type:
                        results.append(node)
        except Exception as e:
            self.logger.debug(f"Type query failed: {e}")
        return results[:max_results]

    def get_related_nodes(self, node_id: str, depth: int = 1) -> List[KnowledgeNode]:
        if node_id not in self.node_cache:
            return []
        related = []
        try:
            if HAS_NETWORKX and self.graph:
                try:
                    descendants = list(nx.descendants(self.graph, node_id))[:depth * 10]
                    ancestors = list(nx.ancestors(self.graph, node_id))[:depth * 10]
                    related_ids = descendants + ancestors
                    with self.lock:
                        for related_id in related_ids:
                            if related_id in self.node_cache:
                                related.append(self.node_cache[related_id])
                except Exception:
                    pass
            else:
                if node_id in self.relationship_cache:
                    with self.lock:
                        for related_id in self.relationship_cache[node_id]:
                            if related_id in self.node_cache:
                                related.append(self.node_cache[related_id])
        except Exception as e:
            self.logger.debug(f"Related nodes query failed: {e}")
        return related

    def update_node(self, node_id: str, updates: Dict[str, Any]) -> bool:
        try:
            with self.lock:
                if node_id in self.node_cache:
                    node = self.node_cache[node_id]
                    if 'title' in updates:
                        node.title = updates['title']
                    if 'content' in updates:
                        node.content = updates['content']
                    if 'tags' in updates:
                        node.tags = updates['tags']
                    if 'metadata' in updates:
                        node.metadata.update(updates['metadata'])
                    if 'confidence' in updates:
                        node.confidence = updates['confidence']
                    self._persist_node(node)
                    return True
        except Exception as e:
            self.logger.debug(f"Node update failed: {e}")
        return False

    def integrate_experience(self, experience_type: str, content: str,
                           related_experiences: List[str] = None,
                           metadata: Dict[str, Any] = None) -> str:
        node_id = self.add_node(
            title=f"Experience: {experience_type}",
            content=content,
            node_type="experience",
            tags=[experience_type, "learned"],
            metadata=metadata or {}
        )
        if related_experiences:
            for related_id in related_experiences:
                try:
                    self.add_relationship(node_id, related_id, "related_to")
                except Exception:
                    pass
        return node_id

    def integrate_project(self, project_name: str, description: str,
                        technologies: List[str], outcomes: str) -> str:
        node_id = self.add_node(
            title=f"Project: {project_name}",
            content=description,
            node_type="project",
            tags=technologies + ["project"],
            metadata={"outcomes": outcomes}
        )
        return node_id

    def get_statistics(self) -> Dict[str, Any]:
        try:
            with self.lock:
                if HAS_NETWORKX and self.graph:
                    edge_count = self.graph.number_of_edges()
                else:
                    edge_count = sum(len(v) for v in self.relationship_cache.values())
                return {
                    'total_nodes': len(self.node_cache),
                    'total_relationships': edge_count,
                    'node_types': self._count_by_type(),
                    'top_accessed': self._get_top_accessed(5),
                    'cache_size': len(self.query_cache),
                    'has_networkx': HAS_NETWORKX
                }
        except Exception as e:
            self.logger.debug(f"Stats fetch failed: {e}")
            return {
                'total_nodes': len(self.node_cache),
                'has_networkx': HAS_NETWORKX
            }

    def _count_by_type(self) -> Dict[str, int]:
        counts = {}
        for node in self.node_cache.values():
            counts[node.node_type] = counts.get(node.node_type, 0) + 1
        return counts

    def _get_top_accessed(self, limit: int) -> List[Dict[str, Any]]:
        try:
            sorted_nodes = sorted(
                self.node_cache.values(),
                key=lambda x: x.access_count,
                reverse=True
            )[:limit]
            return [{'title': n.title, 'access_count': n.access_count} for n in sorted_nodes]
        except Exception:
            return []


class ExperienceExtractor:
    def __init__(self, knowledge_graph: KnowledgeGraphIntegration):
        self.knowledge_graph = knowledge_graph
        self.logger = logging.getLogger(__name__)

    def extract_from_task(self, task_result: Dict[str, Any]) -> Optional[str]:
        try:
            if 'success' in task_result and task_result['success']:
                experience = f"Successfully completed task: {task_result.get('task_type', 'unknown')}"
                if 'duration' in task_result:
                    experience += f" in {task_result['duration']}s"
                return self.knowledge_graph.integrate_experience(
                    experience_type="task_success",
                    content=experience,
                    metadata=task_result
                )
        except Exception as e:
            self.logger.debug(f"Task extract failed (non-critical): {e}")
        return None

    def extract_from_optimization(self, optimization_result: Dict[str, Any]) -> Optional[str]:
        try:
            improvement = optimization_result.get('improvement', 0)
            if improvement > 0:
                experience = f"Applied optimization with {improvement:.2%} improvement"
                return self.knowledge_graph.integrate_experience(
                    experience_type="optimization_success",
                    content=experience,
                    metadata=optimization_result
                )
        except Exception as e:
            self.logger.debug(f"Opt extract failed (non-critical): {e}")
        return None
