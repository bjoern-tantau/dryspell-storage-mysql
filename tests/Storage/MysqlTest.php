<?php
namespace Dryspell\Tests\Storage;

use Dryspell\Storage\Mysql;
use Dryspell\Storage\StorageInterface;
use Dryspell\Storage\StorageSetupInterface;
use Dryspell\Storage\Tests\AbstractTest;
use PDO;

/**
 * MysqlTest
 *
 * @author Björn Tantau <bjoern@bjoern-tantau.de>
 */
class MysqlTest extends AbstractTest
{

    private $pdo;
    private $storage;

    private function getPdo(): PDO
    {
        if (!$this->pdo) {
            $this->pdo = new PDO($GLOBALS['DB_DSN'], $GLOBALS['DB_USER'], $GLOBALS['DB_PASS'], []);
        }
        return $this->pdo;
    }

    protected function getSetupStorage(): StorageSetupInterface
    {
        return $this->getStorage();
    }

    protected function getStorage(): StorageInterface
    {
        if (!$this->storage) {
            $this->storage = new Mysql($this->getPdo());
        }
        return $this->storage;
    }

    public function testSetup()
    {
        $this->getPdo()->exec('DROP TABLE IF EXISTS test_object');
        $this->getPdo()->exec('DROP TABLE IF EXISTS test_parent_object');
        $this->getPdo()->exec('CREATE TABLE `test_object` (`name` VARCHAR(255) NULL, `foobar` VARCHAR(255) NULL, `id` int(10) unsigned NOT NULL AUTO_INCREMENT, `created_at` datetime DEFAULT current_timestamp() NOT NULL, PRIMARY KEY (`id`))');
        parent::testSetup();
        $tableDefinition = $this->getPdo()->query('SHOW CREATE TABLE test_object')->fetch();
        $this->assertEquals('CREATE TABLE `test_object` (
  `name` varchar(255) NOT NULL,
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `parent_id` int(10) unsigned NOT NULL,
  `updated_at` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `parent_id` (`parent_id`),
  CONSTRAINT `fk_test_parent_object_parent_id` FOREIGN KEY (`parent_id`) REFERENCES `test_parent_object` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4', $tableDefinition['.Create Table']);
        $tableDefinition = $this->getPdo()->query('SHOW CREATE TABLE test_parent_object')->fetch();
        $this->assertEquals('CREATE TABLE `test_parent_object` (
  `name` varchar(255) NOT NULL,
  `nullable` varchar(255) DEFAULT NULL,
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `updated_at` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4', $tableDefinition['.Create Table']);
    }
}
