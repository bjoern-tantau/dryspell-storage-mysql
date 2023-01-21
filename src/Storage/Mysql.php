<?php
namespace Dryspell\Storage;

use Dryspell\Models\ObjectInterface;
use Dryspell\Models\Options;
use PDO;
use PDOException;
use ReflectionClass;

/**
 * Mysql
 *
 * @author Björn Tantau <bjoern@bjoern-tantau.de>
 */
class Mysql implements StorageInterface, StorageSetupInterface
{

    const TINYINT_MAX     = 255;
    const INT_MAX         = 4_294_967_296;
    const VARCHAR_DEFAULT = 255;
    const VARCHAR_MAX     = 535;
    const TEXT_MAX        = 65_535;

    public function __construct(public PDO $pdo)
    {
        $stmt = $this->pdo->prepare('SET time_zone = ?');
        $stmt->execute([date_default_timezone_get()]);

        $this->pdo->setAttribute(PDO::ATTR_FETCH_TABLE_NAMES, true);
    }

    public function delete(ObjectInterface $entity): StorageInterface
    {
        $idProperty = $entity::getIdProperty();
        $id         = $entity->$idProperty;
        $tableName  = $this->getTableName(get_class($entity));

        $query = "DELETE FROM `$tableName` WHERE `$idProperty` = ?";
        $stmt  = $this->pdo->prepare($query);
        $stmt->execute([$id]);

        return $this;
    }

    public function find(string $className): FindInterface
    {
        return new Mysql\Find(entityName: $className, mysql: $this);
    }

    public function save(ObjectInterface $entity): StorageInterface
    {
        $idProperty = $entity::getIdProperty();
        $tableName  = $this->getTableName(get_class($entity));
        $values     = $this->getValuesToInsert($entity);
        $columns    = join(', ', array_keys($values));
        $parameters = join(', ', array_map(function ($value) {
                return ':' . $value;
            }, array_keys($values)));
        $onDuplicate = join(', ', array_map(function ($value) {
                return "$value = VALUES($value)";
            }, array_keys($values)));
        $query = "INSERT INTO `$tableName` ($columns) VALUES ($parameters) ON DUPLICATE KEY UPDATE $onDuplicate";

        $stmt   = $this->pdo->prepare($query);
        $stmt->execute($values);
        $lastId = $this->pdo->lastInsertId();

        $newEntities = iterator_to_array(
            $this->find(get_class($entity))
                ->where($idProperty)->equals($lastId)
                ->limit(1)
        );
        $entity->setValues($newEntities[0]->getValues());

        return $this;
    }

    private function getValuesToInsert(ObjectInterface $entity): array
    {
        $properties = $entity->getProperties();
        $values     = $entity->getValues();
        foreach ($values as $name => &$value) {
            if (!isset($properties[$name])) {
                unset($values[$name]);
                continue;
            }
            if ($properties[$name]->onUpdate === 'now') {
                unset($values[$name]);
                continue;
            }
            if ($properties[$name]->default === 'now') {
                unset($values[$name]);
                continue;
            }
            if ($value instanceof \DateTime) {
                $value = $value->format("Y-m-d H:i:s");
            }
            if ($value instanceof ObjectInterface) {
                unset($values[$name]);
                $idProperty            = $value::getIdProperty();
                $values[$name . '_id'] = $value->$idProperty;
            } elseif (is_subclass_of($properties[$name]->type, ObjectInterface::class)) {
                unset($values[$name]);
                $values[$name . '_id'] = $value;
            }
        }
        return $values;
    }

    public function setup(string $entityName): StorageSetupInterface
    {
        $class  = new ReflectionClass($entityName);
        /* @var $object ObjectInterface */
        $object = $class->newInstanceWithoutConstructor();

        $properties = $object->getProperties();

        // Create foreign tables first
        foreach ($properties as $option) {
            if (is_subclass_of($option->type, ObjectInterface::class) && $option->type != $entityName) {
                $this->setup($option->type);
            }
        }

        $tableName = $this->getTableName($entityName);
        try {
            $tableDefinition = $this->pdo->query("SHOW CREATE TABLE `$tableName`")->fetch();
        } catch (PDOException $e) {
            if ($e->getCode() !== '42S02') {
                throw $e;
            }
        }

        if (empty($tableDefinition)) {
            $query = $this->getCreateTableStatement($tableName, $properties);
            $this->pdo->exec($query);
        } else {
            $existingDefinitions = $this->extractDefinitions($tableDefinition['.Create Table']);
            $newDefinitions      = $this->extractDefinitions($this->getCreateTableStatement($tableName, $properties));

            $diff       = array_diff_assoc($newDefinitions, $existingDefinitions);
            $operations = [];
            foreach ($existingDefinitions as $name => $definition) {
                if (!isset($newDefinitions[$name])) {
                    $operations[] = "DROP $name";
                }
            }
            foreach ($diff as $name => $definition) {
                if (isset($existingDefinitions[$name])) {
                    if (str_starts_with($name, 'COLUMN')) {
                        preg_match('/`[^`]+`/', $definition, $matches);
                        $definition = "$name $definition";
                    }
                    $operations[] = "CHANGE $definition";
                } else {
                    $operations[] = "ADD $definition";
                }
            }
            $operationString = join(', ', $operations);
            $query           = "ALTER TABLE `$tableName` $operationString";
            $this->pdo->exec($query);
        }

        return $this;
    }

    public function getTableName(string $entityName): string
    {
        $className = substr($entityName, strrpos($entityName, '\\') + 1);
        return strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $className));
    }

    private function getCreateTableStatement(string $tableName, array $properties): string
    {
        $propertyDefinitions = [];
        foreach ($properties as $property => $options) {
            $propertyDefinitions[] = $this->getDefinition($property, $options);
        }
        $definitionString = join(', ', $propertyDefinitions);
        return "CREATE TABLE `$tableName` ($definitionString)";
    }

    private function extractDefinitions(string $statement): array
    {
        if (!preg_match('/^[^\(]+\((.+)\)[^\)]*$/s', $statement, $matches)) {
            return [];
        }

        $definitions = explode(',', $matches[1]);
        $definitions = array_map('trim', $definitions);
        $results     = [];

        foreach ($definitions as $definition) {
            preg_match('/`[^`]+`/', $definition, $matches);
            $name = $matches[0];

            if (str_starts_with($definition, '`')) {
                $name = "COLUMN $name";
            } elseif (str_starts_with($definition, 'PRIMARY')) {
                $name = 'PRIMARY KEY';
            } elseif (str_starts_with($definition, 'KEY')) {
                $name = "KEY $name";
            } elseif (str_starts_with($definition, 'CONSTRAINT')) {
                $name = "FOREIGN KEY $name";
            }

            $results[$name] = $definition;
        }

        return $results;
    }

    private function getDefinition(string $property, Options $options): string
    {
        $definition = '';
        switch ($options->type) {
            case 'bool':
            case 'boolean':
                $options->length = 1;
                $options->signed = false;
            case 'int':
            case 'integer':
                $definition      .= $this->getIntegerDefinition($property, $options);
                break;
            case 'float':
                $definition      .= $this->getFloatDefinition($property, $options);
                break;
            case 'string':
                $definition      .= $this->getStringDefinition($property, $options);
                break;
            case 'array':
                $definition      .= $this->getArrayDefinition($property, $options);
                break;
            case '\\DateTime':
                $definition      .= $this->getDateTimeDefinition($property, $options);
                break;
            default:
                $definition      .= $this->getObjectDefinition($property, $options);
                break;
        }

        if ($options->id) {
            $definition .= ", PRIMARY KEY (`$property`)";
        }

        if ($options->searchable || is_subclass_of($options->type, ObjectInterface::class)) {
            $definition .= ", KEY `$property` (`$property`)";
        }

        if ($options->unique) {
            $definition .= ", UNIQUE KEY `$property` (`$property`)";
        }

        if (is_subclass_of($options->type, ObjectInterface::class)) {
            $foreingProperty = call_user_func($options->type . '::getIdProperty');
            $definition      .= ", CONSTRAINT `fk_{$this->getTableName($options->type)}_{$property}`"
                . " FOREIGN KEY (`$property`)"
                . " REFERENCES `{$this->getTableName($options->type)}` (`$foreingProperty`)";
            if (!is_null($options->onDelete)) {
                $definition .= " ON DELETE {$options->onDelete}";
            }
            if (!is_null($options->onUpdate)) {
                $definition .= " ON UPDATE {$options->onUpdate}";
            }
        }

        return $definition;
    }

    private function getIntegerDefinition(string $property, Options $options)
    {
        $definition = "`$property`";
        if (is_null($options->length)) {
            $definition .= " int(10)";
        } else {
            if ($options->length <= self::TINYINT_MAX) {
                $definition .= " tinyint(4)";
            } elseif ($options->length <= self::INT_MAX) {
                $definition .= " int(10)";
            } else {
                $definition .= " bigint(15)";
            }
        }
        if (!$options->signed) {
            $definition .= " unsigned";
        }
        if ($options->nullable || !is_null($options->default)) {
            $definition .= " DEFAULT";
        }
        if (!is_null($options->default)) {
            $definition .= " " . $this->pdo->quote($options->default);
        }
        $definition .= " " . ($options->nullable ? 'NULL' : 'NOT NULL');
        if ($options->generatedValue) {
            $definition .= " AUTO_INCREMENT";
        }
        return $definition;
    }

    private function getFloatDefinition(string $property, Options $options)
    {
        $definition = "`$property` float";
        if (!$options->signed) {
            $definition .= " unsigned";
        }
        if ($options->nullable || !is_null($options->default)) {
            $definition .= " DEFAULT";
        }
        if (!is_null($options->default)) {
            $definition .= " " . $this->pdo->quote($options->default);
        }
        $definition .= " " . ($options->nullable ? 'NULL' : 'NOT NULL');
        return $definition;
    }

    private function getStringDefinition(string $property, Options $options)
    {
        $definition = "`$property`";
        if (is_null($options->length)) {
            $definition .= " varchar("
                . self::VARCHAR_DEFAULT
                . ")";
        } else {
            if ($options->length <= self::VARCHAR_MAX) {
                $definition .= " varchar";
                $definition .= "({$options->length})";
            } elseif ($options->length <= self::TEXT_MAX) {
                $definition .= " text";
            } else {
                $definition .= " longtext";
            }
        }
        if ($options->nullable || !is_null($options->default)) {
            $definition .= " DEFAULT";
        }
        if (!is_null($options->default)) {
            $definition .= " " . $this->pdo->quote($options->default);
        }
        $definition .= " " . ($options->nullable ? 'NULL' : 'NOT NULL');
        return $definition;
    }

    private function getArrayDefinition(string $property, Options $options)
    {
        $definition = "`$property` json";
        if ($options->nullable || !is_null($options->default)) {
            $definition .= " DEFAULT";
        }
        if (!is_null($options->default)) {
            $definition .= " " . $this->pdo->quote($options->default);
        }
        $definition .= " " . ($options->nullable ? 'NULL' : 'NOT NULL');
        return $definition;
    }

    private function getDateTimeDefinition(string $property, Options $options)
    {
        $definition = "`$property` datetime";
        $definition .= " " . ($options->nullable ? 'NULL' : 'NOT NULL');
        if ($options->default === 'now') {
            $definition .= " DEFAULT current_timestamp()";
        }
        if ($options->onUpdate === 'now') {
            $definition .= " ON UPDATE current_timestamp()";
        }
        return $definition;
    }

    private function getObjectDefinition(string &$property, Options $options)
    {
        if (is_subclass_of($options->type, ObjectInterface::class)) {
            $property   .= '_id';
            $definition = "`$property` int(10) unsigned";
        } else {
            $definition = "`$property` blob";
        }
        if ($options->nullable || !is_null($options->default)) {
            $definition .= " DEFAULT";
        }
        if (!is_null($options->default)) {
            $definition .= " " . $this->pdo->quote($options->default);
        }
        $definition .= " " . ($options->nullable ? 'NULL' : 'NOT NULL');
        return $definition;
    }
}
