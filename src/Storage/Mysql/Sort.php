<?php
namespace Dryspell\Storage\Mysql;

use Dryspell\Storage\FindInterface;
use Dryspell\Storage\SortInterface;

/**
 * Sort
 *
 * @author Björn Tantau <bjoern@bjoern-tantau.de>
 */
class Sort implements SortInterface
{

    public string $direction = 'ASC';

    public function __construct(public string $name, private FindInterface $find)
    {

    }

    public function ascending(): FindInterface
    {
        $this->direction = 'ASC';
        return $this->find;
    }

    public function descending(): FindInterface
    {
        $this->direction = 'DESC';
        return $this->find;
    }
}
