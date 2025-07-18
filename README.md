# PostgreSQL Database Engine - Implémentation en Go

## 🎯 Aperçu du projet

Ce projet implémente un **database engine** compatible PostgreSQL développé en Go. Il fournit un **RDBMS** (Relational Database Management System) complet avec **SQL parser**, stockage, transactions, indexation et contraintes d'intégrité référentielle.

---

## 🗂️ Navigation rapide du code

| Fonctionnalité / Démo                | Fichier                                 | Lignes clés         | Description rapide |
|--------------------------------------|-----------------------------------------|---------------------|--------------------|
| **Entrée principale CLI**            | [`main.go`](main.go)                    | [main](main.go#L14), [runDemo](main.go#L224) | Point d'entrée, gestion des flags, exécution SQL, affichage résultats |
| **Démo complète (features)**         | [`demo.go`](demo.go)                    | [runComprehensiveDemo](demo.go#L15), [demoCRUDOperations](demo.go#L528), [demoSQLParser](demo.go#L704), [demoConstraints](demo.go#L901), [demoIndexes](demo.go#L1092), [demoViews](demo.go#L1120), [demoJoins](demo.go#L1251), [demoAggregates](demo.go#L1295), [demoTransactions](demo.go#L1394), [demoPerformance](demo.go#L1436), [demoAdvancedFeatures](demo.go#L1496) | Démo guidée de toutes les fonctionnalités |
| **Démo SQL Parser**                  | [`examples/sql-demo/main.go`](examples/sql-demo/main.go) | [main](examples/sql-demo/main.go#L11), [printTableResult](examples/sql-demo/main.go#L209) | Démonstration parser SQL, DDL/DML, index, types, requêtes complexes |
| **Démo Fonctions d'Agrégat**         | [`examples/aggregate-demo/main.go`](examples/aggregate-demo/main.go) | [main](examples/aggregate-demo/main.go#L10), [testAggregateParsing](examples/aggregate-demo/main.go#L20), [testAggregateOperator](examples/aggregate-demo/main.go#L92) | Parsing et exécution d'agrégats, tests GROUP BY/HAVING, opérateurs |
| **Engine principal**                 | [`internal/engine/engine.go`](internal/engine/engine.go) | [NewPostgresEngine](internal/engine/engine.go#L51), [Insert](internal/engine/engine.go#L122), [Select](internal/engine/engine.go#L177), [Update](internal/engine/engine.go#L210), [Delete](internal/engine/engine.go#L289), [CreateDatabase](internal/engine/engine.go#L536), [CreateTable](internal/engine/engine.go#L595), [CreateIndex](internal/engine/engine.go#L617), [BeginTransaction](internal/engine/engine.go#L653), [ExecuteSQL](internal/engine/engine.go#L1342) | Initialisation, gestion des bases, exécution requêtes |
| **Parser SQL**                       | [`internal/parser/parser.go`](internal/parser/parser.go) | [NewParser](internal/parser/parser.go#L25), [Parse](internal/parser/parser.go#L35), [parseCreateTable](internal/parser/parser.go#L149), [parseSelectStatement](internal/parser/parser.go#L394), [parseInsertStatement](internal/parser/parser.go#L501), [parseUpdateStatement](internal/parser/parser.go#L584), [parseDeleteStatement](internal/parser/parser.go#L643) | Parsing SQL, AST, gestion erreurs |
| **Lexer SQL**                        | [`internal/parser/lexer.go`](internal/parser/lexer.go) | [NewLexer](internal/parser/lexer.go#L20), [NextToken](internal/parser/lexer.go#L57), [GetAllTokens](internal/parser/lexer.go#L324) | Découpage lexical SQL |
| **Types SQL**                        | [`internal/types/types.go`](internal/types/types.go) | [DataType](internal/types/types.go#L10), [Column](internal/types/types.go#L130), [Schema](internal/types/types.go#L142), [Table](internal/types/types.go#L196), [GetTypeInfo](internal/types/types.go#L426), [IsNumericType](internal/types/types.go#L544) | Système de types, définitions |
| **Stockage**                         | [`internal/storage/storage.go`](internal/storage/storage.go) | [NewStorageManager](internal/storage/storage.go#L156), [CreateTable](internal/storage/storage.go#L189), [InsertTuple](internal/storage/storage.go#L214), [SelectTuple](internal/storage/storage.go#L310), [UpdateTuple](internal/storage/storage.go#L361), [DeleteTuple](internal/storage/storage.go#L450) | Interface stockage |
| **Stockage binaire**                 | [`internal/storage/binary_storage.go`](internal/storage/binary_storage.go) | ... | Implémentation stockage binaire |
| **Index B-Tree**                     | [`internal/index/btree.go`](internal/index/btree.go) | [NewBTree](internal/index/btree.go#L32), [Insert](internal/index/btree.go#L71), [Search](internal/index/btree.go#L157), [Delete](internal/index/btree.go#L190), [RangeScan](internal/index/btree.go#L236) | Indexation B-Tree |
| **Transactions**                     | [`internal/transaction/transaction.go`](internal/transaction/transaction.go) | [NewTransactionManager](internal/transaction/transaction.go#L22), [Begin](internal/transaction/transaction.go#L37), [Commit](internal/transaction/transaction.go#L66), [Rollback](internal/transaction/transaction.go#L98) | Gestion transactions, isolation |
| **Tests parser SQL**                 | [`tests/sql_parser_test.go`](tests/sql_parser_test.go) | ... | Tests lexer, parser, intégration |
| **Tests vues**                       | [`tests/views_test.go`](tests/views_test.go) | ... | TDD sur les vues |
| **Tests agrégats**                   | [`tests/aggregate_functions_test.go`](tests/aggregate_functions_test.go) | ... | Tests fonctions d'agrégat |

> **Astuce navigation** : Utilisez la recherche par nom de fichier et ligne pour accéder rapidement à la logique souhaitée.

### Liens directs importants

- **Engine** :
  - [NewPostgresEngine](internal/engine/engine.go#L51) · [Insert](internal/engine/engine.go#L122) · [Select](internal/engine/engine.go#L177) · [Update](internal/engine/engine.go#L210) · [Delete](internal/engine/engine.go#L289) · [CreateDatabase](internal/engine/engine.go#L536) · [CreateTable](internal/engine/engine.go#L595) · [CreateIndex](internal/engine/engine.go#L617) · [BeginTransaction](internal/engine/engine.go#L653) · [ExecuteSQL](internal/engine/engine.go#L1342)
- **Parser** :
  - [NewParser](internal/parser/parser.go#L25) · [Parse](internal/parser/parser.go#L35) · [parseCreateTable](internal/parser/parser.go#L149) · [parseSelectStatement](internal/parser/parser.go#L394) · [parseInsertStatement](internal/parser/parser.go#L501) · [parseUpdateStatement](internal/parser/parser.go#L584) · [parseDeleteStatement](internal/parser/parser.go#L643)
- **Lexer** :
  - [NewLexer](internal/parser/lexer.go#L20) · [NextToken](internal/parser/lexer.go#L57) · [GetAllTokens](internal/parser/lexer.go#L324)
- **Types** :
  - [DataType](internal/types/types.go#L10) · [Column](internal/types/types.go#L130) · [Schema](internal/types/types.go#L142) · [Table](internal/types/types.go#L196) · [GetTypeInfo](internal/types/types.go#L426) · [IsNumericType](internal/types/types.go#L544)
- **Stockage** :
  - [NewStorageManager](internal/storage/storage.go#L156) · [CreateTable](internal/storage/storage.go#L189) · [InsertTuple](internal/storage/storage.go#L214) · [SelectTuple](internal/storage/storage.go#L310) · [UpdateTuple](internal/storage/storage.go#L361) · [DeleteTuple](internal/storage/storage.go#L450)
- **Index B-Tree** :
  - [NewBTree](internal/index/btree.go#L32) · [Insert](internal/index/btree.go#L71) · [Search](internal/index/btree.go#L157) · [Delete](internal/index/btree.go#L190) · [RangeScan](internal/index/btree.go#L236)
- **Transactions** :
  - [NewTransactionManager](internal/transaction/transaction.go#L22) · [Begin](internal/transaction/transaction.go#L37) · [Commit](internal/transaction/transaction.go#L66) · [Rollback](internal/transaction/transaction.go#L98)

---

## ✨ Fonctionnalités principales

### 🔤 SQL Parser
- **Lexer** complet avec tokenisation SQL ([internal/parser/lexer.go])
- **Parser** supportant DDL et DML ([internal/parser/parser.go])
- **AST** (Abstract Syntax Tree) ([internal/parser/ast.go])
- **Gestion d'erreurs** détaillée

### 🗄️ Database Management
- Création/suppression de bases ([internal/engine/engine.go])
- Gestion des **schemas** ([internal/types/types.go])
- Support multi-database

### 📊 PostgreSQL Data Types
- Types supportés : voir [internal/types/types.go]

### 🔧 SQL Operations
- DDL/DML : CREATE, INSERT, SELECT, UPDATE, DELETE ([main.go], [demo.go])
- Vues, contraintes, agrégats, jointures, sous-requêtes ([demo.go], [examples/sql-demo/main.go])

### 🚀 Performance Optimizations
- Stockage binaire ([internal/storage/binary_storage.go])
- Index B-Tree ([internal/index/btree.go])
- Transactions ([internal/transaction/transaction.go])

### 🔒 Data Integrity
- Contraintes référentielles, validation types, ACID ([demo.go])

---

## 🏗️ Architecture du projet

```
PostgresInGo/
├── main.go                    # Entrée CLI principale (voir lignes 1-180)
├── demo.go                    # Démo guidée de toutes les features (voir lignes 1-900)
├── internal/
│   ├── engine/               # Moteur principal
│   │   └── engine.go         # Initialisation, gestion bases
│   ├── parser/               # Sous-système SQL Parser
│   │   ├── tokens.go         # Définition tokens SQL
│   │   ├── lexer.go          # Analyseur lexical
│   │   ├── ast.go            # AST
│   │   └── parser.go         # Parser SQL
│   ├── execution/            # Exécution requêtes
│   │   └── execution.go      # Opérateurs, plans
│   ├── storage/              # Stockage
│   │   ├── storage.go        # Interface stockage
│   │   └── binary_storage.go # Stockage binaire
│   ├── index/                # Indexation
│   │   └── btree.go          # B-Tree
│   ├── transaction/          # Transactions
│   │   └── transaction.go    # Isolation, contrôle
│   └── types/                # Système de types
│       ├── types.go          # Types SQL
│       └── types_test.go     # Tests types
├── tests/                    # Tests
│   ├── sql_parser_test.go    # Tests parser SQL
│   ├── views_test.go         # Tests vues
│   ├── aggregate_functions_test.go # Tests agrégats
│   └── ...
├── examples/
│   ├── sql-demo/main.go      # Démo parser SQL
│   └── aggregate-demo/main.go # Démo agrégats
├── data/                     # Données
└── demo_data/                # Données démo
```

---

## 🚀 Installation et utilisation

### Prérequis
- Go 1.24.1 ou supérieur

### Installation
```bash
git clone <repository-url>
cd PostgresInGo
go mod tidy
```

### Exécution
```bash
go run main.go
```

### Exemples d'utilisation
- **Démo complète** : `go run demo.go` ([demo.go])
- **Démo SQL Parser** : `go run examples/sql-demo/main.go` ([examples/sql-demo/main.go])
- **Démo Agrégats** : `go run examples/aggregate-demo/main.go` ([examples/aggregate-demo/main.go])

---

## 🧪 Tests

- **Tests parser SQL** : `go test ./tests/sql_parser_test.go -v`
- **Tests vues** : `go test ./tests/views_test.go -v`
- **Tests agrégats** : `go test ./tests/aggregate_functions_test.go -v`
- **Tous les tests** : `go test ./...`

---

## 📖 Pour aller plus loin

- Consultez les fichiers référencés dans le tableau ci-dessus pour explorer chaque fonctionnalité.
- Les démos (`demo.go`, `examples/sql-demo/main.go`, `examples/aggregate-demo/main.go`) montrent l'utilisation réelle du moteur, du parser, des contraintes, des index, des vues, etc.
- Les tests dans `tests/` couvrent tous les cas d'usage et edge cases.

---

## 📊 Statistiques du projet

- **Modules** : 7 modules principaux (+ SQL Parser)
- **Types supportés** : 50+ types PostgreSQL
- **Tokens SQL** : 100+ tokens supportés
- **Commandes SQL** : Support complet DDL/DML avec parser + Views
- **Tests** : 800+ lignes de tests pour le SQL parser et Views
- **Fonctionnalités SQL** : CREATE, INSERT, SELECT, UPDATE, DELETE, WHERE, INDEX, CREATE VIEW, DROP VIEW