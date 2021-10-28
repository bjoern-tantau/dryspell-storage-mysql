<?php
namespace Dryspell\Storage\Mysql;

use Dryspell\Storage\FindInterface;
use Dryspell\Storage\WhereInterface;

/**
 * Where
 *
 * @author Björn Tantau <bjoern@bjoern-tantau.de>
 *
 * @property ?string $value
 */
class Where implements WhereInterface
{

    public ?string $operator;
    private ?string $realValue;

    public function __construct(public string $name, private Find $find)
    {

    }

    public function startsWith($value): FindInterface
    {
        $this->operator = 'LIKE';
        $this->value    = "$value%";
        return $this->find;
    }

    public function contains($value): FindInterface
    {
        $this->operator = 'LIKE';
        $this->value    = "%$value%";
        return $this->find;
    }

    public function endsWith($value): FindInterface
    {
        $this->operator = 'LIKE';
        $this->value    = "%$value";
        return $this->find;
    }

    public function equals($value): FindInterface
    {
        $this->operator = '=';
        $this->value    = $value;
        return $this->find;
    }

    public function equalsOneOf(array $values): FindInterface
    {
        foreach ($values as &$value) {
            $value = $this->sanitizeValue($value);
            $value = $this->find->mysql->pdo->quote($value);
        }
        $values         = join(', ', $values);
        $this->operator = "IN($values)";
        $this->value    = null;
        return $this->find;
    }

    public function isGreaterThan($value): FindInterface
    {
        $this->operator = '>';
        $this->value    = $value;
        return $this->find;
    }

    public function isGreaterThanOrEquals($value): FindInterface
    {
        $this->operator = '>=';
        $this->value    = $value;
        return $this->find;
    }

    public function isLowerThan($value): FindInterface
    {
        $this->operator = '<';
        $this->value    = $value;
        return $this->find;
    }

    public function isLowerThanOrEquals($value): FindInterface
    {
        $this->operator = '<=';
        $this->value    = $value;
        return $this->find;
    }

    public function isNotNull(): FindInterface
    {
        $this->operator = 'IS NOT NULL';
        $this->value    = null;
        return $this->find;
    }

    public function isNull(): FindInterface
    {
        $this->operator = 'IS NULL';
        $this->value    = null;
        return $this->find;
    }

    public function __get($name)
    {
        if ($name === 'value') {
            return $this->realValue;
        }
    }

    public function __set($name, $value)
    {
        if ($name !== 'value') {
            return;
        }
        $value           = $this->sanitizeValue($value);
        $this->realValue = $value;
    }

    public function __isset($name)
    {
        if ($name !== 'value') {
            return;
        }
        return isset($this->realValue);
    }

    public function __unset($name)
    {
        if ($name !== 'value') {
            return;
        }
        unset($this->realValue);
    }

    private function sanitizeValue($value): ?string
    {
        if ($value instanceof \DateTime) {
            $value = $value->format(\DateTime::ATOM);
        }
        if ($value instanceof \Dryspell\Models\ObjectInterface) {
            $idProperty = $value::getIdProperty();
            $value      = $value->$idProperty;
        }
        return $value;
    }
}
