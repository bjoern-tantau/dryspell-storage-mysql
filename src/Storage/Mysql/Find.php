<?php
namespace Dryspell\Storage\Mysql;

use Dryspell\Models\ObjectInterface;
use Dryspell\Models\Options;
use Dryspell\Storage\FindInterface;
use Dryspell\Storage\Mysql;
use Dryspell\Storage\SortInterface;
use Dryspell\Storage\WhereInterface;
use PDO;
use Traversable;

/**
 * Find
 *
 * @author Björn Tantau <bjoern@bjoern-tantau.de>
 */
class Find implements FindInterface
{

    /**
     *
     * @var Where[]
     */
    private array $wheres = [];

    /**
     *
     * @var Sort[]
     */
    private array $sorts = [];

    /**
     *
     * @var Find[]
     */
    private array $withs = [];

    /**
     * How many rows to return
     *
     * @var int
     */
    private int $count = \PHP_INT_MAX;

    /**
     * Where to start returning rows
     *
     * @var int
     */
    private int $from = 0;

    public function __construct(
        private $entityName, public Mysql $mysql, private ?Find $parent = null
    )
    {

    }

    public function getIterator(): Traversable
    {
        if (!is_null($this->parent)) {
            yield from $this->parent;
            return;
        }
        /* @var $object ObjectInterface */
        $object    = new $this->entityName;
        $tableName = $this->mysql->getTableName($this->entityName);
        $query     = "SELECT * FROM `$tableName`";

        $binds = [];

        foreach ($this->withs as $propertyName => $with) {
            $query .= ' ' . $with->getJoin($tableName, $propertyName, $binds);
        }

        if (!empty($this->wheres)) {
            $wheres = $this->buildWheres($object, $tableName, $binds);
            $query  .= " WHERE $wheres";
        }

        if (!empty($this->sorts)) {
            $sorts = join(', ', array_map(function (Sort $sort) use ($tableName) {
                    return "`$tableName`.`$sort->name` $sort->direction";
                }, $this->sorts));
            $query .= " ORDER BY $sorts";
        }

        if ($this->count < PHP_INT_MAX || $this->from > 0) {
            $query .= " LIMIT $this->count OFFSET $this->from";
        }

        $stmt = $this->mysql->pdo->prepare($query);
        $stmt->execute($binds);

        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            /* @var $entity ObjectInterface */
            $entity = new $this->entityName;
            $values = $this->getValuesForTable($row, $tableName);
            $entity->setValues($values, weaklyTyped: true);

            if (!empty($with)) {
                foreach ($entity->getProperties() as $propertyName => $options) {
                    if (is_subclass_of($options->type, ObjectInterface::class)) {
                        $values = $this->getValuesForTable($row, "$tableName-$propertyName");
                        if (!empty($values)) {
                            /* @var $subObject ObjectInterface */
                            $subObject             = new $options->type;
                            $subObject->setValues($values, weaklyTyped: true);
                            $entity->$propertyName = $subObject;
                        }
                    }
                }
            }
            yield $entity;
        }
    }

    private function buildWheres(ObjectInterface $object, string $tableName, array &$binds): string
    {
        $properties = $object->getProperties();
        $wheres     = join(' AND ', array_map(function (Where $where) use (&$binds, $properties, $tableName) {
                $name     = "`$tableName`.`$where->name";
                /* @var $property Options */
                $property = $properties[$where->name];
                if (is_subclass_of($property->type, ObjectInterface::class)) {
                    $name .= '_id';
                }
                $name   .= '`';
                $string = "$name $where->operator";
                if (isset($where->value)) {
                    $string  .= ' ?';
                    $binds[] = $where->value;
                }
                return $string;
            }, $this->wheres));
        return $wheres;
    }

    private function getJoin(string $parentTableName, string $propertyName, array &$binds): string
    {
        /* @var $object ObjectInterface */
        $object       = new $this->entityName;
        $tableName    = $this->mysql->getTableName($this->entityName);
        $tableAlias   = "$parentTableName-$propertyName";
        $idProperty   = $object::getIdProperty();
        $propertyName .= '_id';
        $query        = "JOIN `$tableName` AS `$tableAlias` ON `$parentTableName`.`$propertyName` = `$tableAlias`.`$idProperty`";
        $where        = $this->buildWheres($object, $tableAlias, $binds);
        if (!empty($where)) {
            $query .= ' AND ' . $where;
        }
        return $query;
    }

    private function getValuesForTable(array $row, string $tableName): array
    {
        $values = [];
        foreach ($row as $key => $value) {
            if (str_starts_with($key, "$tableName.")) {
                $values[substr($key, strpos($key, ".") + 1)] = $value;
            }
        }
        return $values;
    }

    public function limit(int $count, int $from = 0): FindInterface
    {
        $this->count = $count;
        $this->from  = $from;
        return $this;
    }

    public function sortBy(string $propertyName): SortInterface
    {
        $sort          = new Sort($propertyName, $this);
        $this->sorts[] = $sort;
        return $sort;
    }

    public function where(string $propertyName): WhereInterface
    {
        $where          = new Where($propertyName, $this);
        $this->wheres[] = $where;
        return $where;
    }

    public function with(string $propertyName): FindInterface
    {
        /* @var $object ObjectInterface */
        $object     = new $this->entityName;
        $properties = $object->getProperties();
        $property   = $properties[$propertyName];

        $with                       = new static($property->type, mysql: $this->mysql, parent: $this);
        $this->withs[$propertyName] = $with;
        return $with;
    }
}
