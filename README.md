# PostgreSQL Database Engine - Implémentation en Go

## 🎯 Aperçu du projet

Ce projet implémente un **database engine** compatible PostgreSQL développé en Go. Il fournit un **RDBMS** (Relational Database Management System) complet avec **SQL parser**, stockage, transactions, indexation et contraintes d'intégrité référentielle.

## ✨ Fonctionnalités principales

### 🔤 SQL Parser
- **Lexer** complet avec tokenisation SQL
- **Parser** supportant DDL et DML
- **AST** (Abstract Syntax Tree) pour représentation des requêtes
- **Intégration** transparente avec l'engine
- **Gestion d'erreurs** détaillée avec position des erreurs

### 🗄️ Database Management
- Création et suppression de databases
- Gestion des **schemas** et **metadata**
- Support **multi-database**

### 📊 PostgreSQL Data Types
- **Integer types** : SMALLINT, INT, BIGINT, SERIAL, BIGSERIAL
- **Numeric types** : NUMERIC, DECIMAL, REAL, DOUBLE, FLOAT, MONEY
- **Character types** : CHAR, VARCHAR, TEXT
- **Temporal types** : DATE, TIME, TIMESTAMP, INTERVAL
- **Boolean types** : BOOLEAN
- **JSON types** : JSON, JSONB
- **Network types** : INET, CIDR, MACADDR
- **Geometric types** : POINT, LINE, BOX, CIRCLE, POLYGON
- **UUID types** et **Array types**

### 🔧 SQL Operations
- **DDL** (Data Definition Language) : CREATE TABLE, DROP TABLE, ALTER TABLE
- **DML** (Data Manipulation Language) : INSERT, UPDATE, DELETE, SELECT
- **Views** : CREATE VIEW, DROP VIEW, SELECT FROM VIEW
- **Constraints** : PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
- **Joins** : INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN
- **Aggregations** : COUNT(*), COUNT(column), COUNT(DISTINCT column), SUM, AVG, MIN, MAX, GROUP BY, HAVING
- **Subqueries** et **CTEs** (Common Table Expressions)

### 🚀 Performance Optimizations
- **Binary storage** haute performance
- **B-Tree indexes** pour accès rapide
- **Cache-aligned** structures pour optimisation mémoire
- **Transaction management** avec isolation
- **Connection pooling** efficace

### 🔒 Data Integrity
- **Referential integrity constraints**
- **Data type validation**
- **ACID transactions** compliance
- **Optimistic locking**

## 🏗️ Architecture

```
postgres-engine/
├── main.go                    # Application entry point
├── internal/
│   ├── engine/               # Core database engine
│   │   └── engine.go         # Engine configuration & initialization
│   ├── parser/               # SQL Parser subsystem
│   │   ├── tokens.go         # SQL token definitions
│   │   ├── lexer.go          # SQL lexical analyzer
│   │   ├── ast.go            # Abstract Syntax Tree definitions
│   │   └── parser.go         # SQL parser implementation
│   ├── execution/            # Query execution engine
│   │   └── execution.go      # Execution operators & query plans
│   ├── storage/              # Storage manager
│   │   ├── storage.go        # Storage interface
│   │   └── binary_storage.go # Optimized binary storage
│   ├── index/               # Indexing subsystem
│   │   └── btree.go         # B-Tree index implementation
│   ├── transaction/         # Transaction manager
│   │   └── transaction.go   # Isolation & concurrency control
│   └── types/               # Type system
│       ├── types.go         # Data type definitions
│       └── types_test.go    # Type system tests
├── tests/                   # Test files
│   ├── sql_parser_test.go  # SQL parser comprehensive tests
│   ├── views_test.go       # Views TDD tests (CREATE/DROP/SELECT)
│   ├── views_parsing_test.go # Views parsing unit tests
│   └── ...                 # Other comprehensive tests
├── examples/                # Example applications
│   └── sql_demo.go         # SQL parser demonstration
├── data/                    # Data directory
└── demo_data/              # Demo data samples
```

## 🚀 Installation et utilisation

### Prerequisites
- Go 1.24.1 ou version supérieure

### Installation
```bash
git clone <repository-url>
cd postgres-engine
go mod tidy
```

### Execution
```bash
go run main.go
```

### Usage Example
```go
package main

import (
    "github.com/esgi-git/postgres-engine/internal/engine"
)

func main() {
    // Initialize database engine
    pg, err := engine.NewPostgresEngine("./data")
    if err != nil {
        log.Fatal("Engine initialization failed:", err)
    }
    defer pg.Close()

    // Create database with SQL parser
    result, err := pg.ExecuteSQL("CREATE DATABASE exemple")
    if err != nil {
        log.Fatal("Database creation error:", err)
    }

    // Create table with constraints using SQL parser
    result, err = pg.ExecuteSQL(`
        CREATE TABLE utilisateurs (
            id INT NOT NULL,
            nom VARCHAR(50) NOT NULL,
            email VARCHAR(100),
            age INT,
            PRIMARY KEY (id)
        )
    `)

    // Insert data with SQL parser
    result, err = pg.ExecuteSQL(`
        INSERT INTO utilisateurs (id, nom, email, age) 
        VALUES (1, 'Alice', 'alice@example.com', 25)
    `)

    // Query data with SQL parser
    result, err = pg.ExecuteSQL("SELECT * FROM utilisateurs WHERE age > 20")

    // Create and use views with SQL parser
    result, err = pg.ExecuteSQL(`
        CREATE VIEW utilisateurs_actifs AS 
        SELECT id, nom, email FROM utilisateurs WHERE age >= 18
    `)
    
    // Query from view
    result, err = pg.ExecuteSQL("SELECT * FROM utilisateurs_actifs")
    
    // Use aggregate functions
    result, err = pg.ExecuteSQL("SELECT COUNT(*), AVG(age) FROM utilisateurs")
    
    // Complex aggregates with GROUP BY
    result, err = pg.ExecuteSQL(`
        SELECT 
            CASE WHEN age < 30 THEN 'Young' ELSE 'Senior' END as age_group,
            COUNT(*) as total,
            AVG(age) as avg_age,
            MIN(age) as min_age,
            MAX(age) as max_age
        FROM utilisateurs 
        GROUP BY age_group
        HAVING COUNT(*) > 0
    `)
}
```

## 🧪 Testing

Le projet inclut une **comprehensive test suite** avec tests spécialisés pour le SQL parser :

### Tests du SQL Parser

```bash
# Test complet du SQL parser
go test ./tests/sql_parser_test.go -v

# Test des vues (CREATE/DROP/SELECT FROM VIEW)
go test ./tests/views_test.go -v
go test ./tests/views_parsing_test.go -v

# Test du lexer SQL
go test ./tests/sql_parser_test.go -v -run "TestSQLLexer"

# Test du parser DDL (CREATE, DROP, etc.)
go test ./tests/sql_parser_test.go -v -run "TestSQLParser/DDLStatements"

# Test du parser DML (SELECT, INSERT, etc.)
go test ./tests/sql_parser_test.go -v -run "TestSQLParser/DMLStatements"

# Test d'intégration SQL avec l'engine
go test ./tests/sql_parser_test.go -v -run "TestSQLExecutionIntegration"

# Test des cas limites du parser
go test ./tests/sql_parser_test.go -v -run "TestSQLParserEdgeCases"

# Benchmarks de performance du parser
go test ./tests/sql_parser_test.go -v -run "BenchmarkSQLParser"
```

### Démonstration du SQL Parser

```bash
# Exécuter la démonstration complète
go run examples/sql_demo.go

# Démonstration des fonctions d'agrégats
go run examples/aggregate_demo.go

# Build de l'exemple (vérification compilation)
go build ./examples/sql_demo.go
go build ./examples/aggregate_demo.go
```

### Tests Généraux

```bash
# Run all tests
go test ./...

# Verbose testing
go test -v ./...

# Performance benchmarks
go test -bench=. ./...

# Module-specific tests
go test ./internal/types -v
go test ./internal/storage -v

# Test des fonctions d'agrégats
go test ./tests/aggregate_functions_test.go -v
```

### Test Coverage
- **SQL Parser** : Tests complets du lexer, parser et intégration
- **Views System** : Tests TDD complets (CREATE VIEW, DROP VIEW, SELECT FROM VIEW)
- **Aggregate Functions** : Tests complets des fonctions COUNT, SUM, AVG, MIN, MAX avec GROUP BY/HAVING
- **Unit tests** : Tous les modules (engine, storage, types, etc.)
- **Integration tests** : Opérations SQL complètes avec parser
- **Performance benchmarks** : Load testing et optimisations
- **Edge cases** : Gestion d'erreurs et cas limites SQL

## ⚙️ Configuration

### Modes de stockage
```go
// Configuration du moteur
config := &engine.EngineConfig{
    DataDir:     "./data",
    StorageMode: engine.BinaryStorage, // ou JSONStorage
}
```

### Options de performance
- **BinaryStorage** : Stockage binaire optimisé (recommandé)
- **JSONStorage** : Stockage JSON (compatible, plus lent)
- **Cache aligné** : Optimisation mémoire automatique
- **Index automatiques** : Création d'index sur les clés primaires

## 📈 Performance

### Benchmarks
- **Stockage binaire** : ~10x plus rapide que JSON
- **Index B-Tree** : Recherche O(log n)
- **Transactions** : Support ACID complet
- **Mémoire** : Gestion optimisée avec cache aligné

### Optimisations implémentées
- Sérialisation binaire haute performance
- Structures de données cache-alignées
- Pool de connexions réutilisables
- Indexation automatique des clés primaires

## 🔄 Développement

### Structure du code
- Code modulaire et extensible
- Interfaces bien définies
- Gestion d'erreurs robuste
- Documentation complète

### Standards de qualité
- Tests unitaires exhaustifs
- Gestion des erreurs appropriée
- Code formaté avec `gofmt`
- Respect des conventions Go

## 📊 Statistiques du projet

- **Lignes de code** : ~10,000+ lignes Go
- **Modules** : 7 modules principaux (+ SQL Parser)
- **Types supportés** : 50+ types PostgreSQL
- **Tokens SQL** : 100+ tokens supportés
- **Commandes SQL** : Support complet DDL/DML avec parser + Views
- **Tests** : 800+ lignes de tests pour le SQL parser et Views
- **Fonctionnalités SQL** : CREATE, INSERT, SELECT, UPDATE, DELETE, WHERE, INDEX, CREATE VIEW, DROP VIEW