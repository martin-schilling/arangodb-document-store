<?php

declare(strict_types=1);

namespace App\Infrastructure\DocumentStore;

use ArangoDBClient\Connection;
use ArangoDBClient\ServerException;
use Prooph\EventMachine\Persistence\DocumentStore;
use Prooph\EventMachine\Persistence\DocumentStore\Filter\Filter;
use Prooph\EventMachine\Persistence\DocumentStore\Index;
use Prooph\EventMachine\Persistence\DocumentStore\OrderBy\OrderBy;

final class ArangoDbDocumentStore implements DocumentStore
{
    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var string
     */
    private $collectionPrefix = 'em_ds_';

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function listCollections(): array
    {
        $query = <<<AQL
FOR collection IN COLLECTIONS() 
    FILTER CONTAINS(collection.name, @prefix)
    RETURN collection.name
AQL;

        $result = $this->connection->post('/_api/cursor', \json_encode([
            'query' => $query,
            'bindVars' => [
                'prefix' => $this->collectionPrefix
            ]
        ]));

        $result = \json_decode($result->getBody(), true);
        $collectionNames = $result['result'];

        while ($result['hasMore']) {
            $result = \json_decode($this->connection->put('/_api/cursor/' . $result['id'], '')->getBody(), true);
            $collectionNames = array_merge($collectionNames, $result['result']);
        }

        return array_map(
            function(string $collectionName): string {
                return strtr($collectionName, [$this->collectionPrefix => '']);
            },
            $collectionNames
        );
    }

    public function filterCollectionsByPrefix(string $prefix): array
    {
        $query = <<<AQL
FOR collection IN COLLECTIONS() 
    FILTER CONTAINS(collection.name, @prefix)
    RETURN collection.name
AQL;

        $result = $this->connection->post('/_api/cursor', \json_encode([
            'query' => $query,
            'bindVars' => [
                'prefix' => $this->collectionPrefix . $prefix
            ]
        ]));

        $result = \json_decode($result->getBody(), true);
        $collectionNames = $result['result'];

        while ($result['hasMore']) {
            $result = \json_decode($this->connection->put('/_api/cursor/' . $result['id'], '')->getBody(), true);
            $collectionNames = array_merge($collectionNames, $result['result']);
        }

        return array_map(
            function(string $collectionName): string {
                return strtr($collectionName, [$this->collectionPrefix => '']);
            },
            $collectionNames
        );
    }

    public function hasCollection(string $collectionName): bool
    {
        $query = <<<AQL
FOR collection IN COLLECTIONS() 
    FILTER collection.name == @collectionName
    RETURN collection.name
AQL;

        $result = $this->connection->post('/_api/cursor', \json_encode([
            'query' => $query,
            'bindVars' => [
                'collectionName' => $this->collectionName($collectionName)
            ]
        ]));

        $result = \json_decode($result->getBody(), true);
        $collectionNames = $result['result'];

        return count($collectionNames) > 0;
    }

    public function addCollection(string $collectionName, Index ...$indices): void
    {
        $result = $this->connection->post('/_api/collection', \json_encode([
            'name' => $this->collectionName($collectionName)
        ]));

        $result = \json_decode($result->getBody(), true);

        // @todo indices
    }

    public function dropCollection(string $collectionName): void
    {
        $result = $this->connection->delete('/_api/collection/' . $this->collectionName($collectionName));

        $result = \json_decode($result->getBody(), true);
    }

    public function addDoc(string $collectionName, string $docId, array $doc): void
    {
        $result = $this->connection->post('/_api/document/' . $this->collectionName($collectionName), \json_encode([
            '_key' => $docId,
            'doc' => $doc
        ]));

        $result = \json_decode($result->getBody(), true);
    }

    public function updateDoc(string $collectionName, string $docId, array $docOrSubset): void
    {
        $result = $this->connection->put('/_api/document/' . $this->collectionName($collectionName) . '/' . $docId, \json_encode([
            '_key' => $docId,
            'doc' => $docOrSubset
        ]));

        $result = \json_decode($result->getBody(), true);
    }

    public function updateMany(string $collectionName, Filter $filter, array $set): void
    {
        [$filterStr, $args] = $this->createFilter($filter);

        $query = <<<AQL
FOR d IN @@collection 
    FILTER {$filterStr}
    UPDATE d WITH { doc: @newDoc } IN @@collection
AQL;

        $result = $this->connection->post('/_api/cursor', \json_encode([
            'query' => $query,
            'bindVars' => array_merge(
                [
                    '@collection' => $this->collectionName($collectionName),
                    'newDoc' => $set,
                ],
                $args
            )
        ]));

        $result = \json_decode($result->getBody(), true);
        var_dump($result);
    }

    public function upsertDoc(string $collectionName, string $docId, array $docOrSubset): void
    {
        $doc = $this->getDoc($collectionName, $docId);

        if (null === $doc) {
            $this->addDoc($collectionName, $docId, $docOrSubset);
        } else {
            $this->updateDoc($collectionName, $docId, $docOrSubset);
        }
    }

    public function deleteDoc(string $collectionName, string $docId): void
    {
        $this->connection->delete('/_api/document/' . $this->collectionName($collectionName) . '/' . $docId);
    }

    public function deleteMany(string $collectionName, Filter $filter): void
    {
        // TODO: Implement deleteMany() method.
    }

    public function getDoc(string $collectionName, string $docId): ?array
    {
        try {
            $result = $this->connection->get('/_api/document/' . $this->collectionName($collectionName) . '/' . $docId);
            $result = \json_decode($result->getBody(), true);
        } catch(ServerException $e) {
            if ($e->getCode() === 404) {
                return null;
            }

            throw $e;
        }

        return $result['doc'];
    }

    public function filterDocs(string $collectionName, Filter $filter, int $skip = null, int $limit = null, OrderBy $orderBy = null): \Traversable
    {
        [$filterStr, $args] = $this->createFilter($filter);

        $query = <<<AQL
FOR d IN @@collection 
    FILTER {$filterStr}
    RETURN d
AQL;

        $result = $this->connection->post('/_api/cursor', \json_encode([
            'query' => $query,
            'bindVars' => array_merge(
                [
                    '@collection' => $this->collectionName($collectionName)
                ],
                $args
            )
        ]));

        $result = \json_decode($result->getBody(), true);
        $documents = $result['result'];

        while ($result['hasMore']) {
            $result = \json_decode($this->connection->put('/_api/cursor/' . $result['id'], '')->getBody(), true);
            $documents = array_merge($documents, $result['result']);
        }

        return new \ArrayIterator($documents);
    }

    private function collectionName(string $collectionName): string
    {
        return mb_strtolower($this->collectionPrefix . $collectionName);
    }

    public function createFilter(Filter $filter, $argsCount = 0): array
    {
        if ($filter instanceof DocumentStore\Filter\AnyFilter) {
            if ($argsCount > 0) {
                throw new \InvalidArgumentException('AnyFilter cannot be used together with other filters.');
            }

            return [null, [], $argsCount];
        }

        if ($filter instanceof DocumentStore\Filter\AndFilter) {
            [$filterA, $argsA, $argsCount] = $this->createFilter($filter->aFilter(), $argsCount);
            [$filterB, $argsB, $argsCount] = $this->createFilter($filter->bFilter(), $argsCount);
            return ["({$filterA} && {$filterB})", array_merge($argsA, $argsB), $argsCount];
        }

        if ($filter instanceof DocumentStore\Filter\OrFilter) {
            [$filterA, $argsA, $argsCount] = $this->createFilter($filter->aFilter(), $argsCount);
            [$filterB, $argsB, $argsCount] = $this->createFilter($filter->bFilter(), $argsCount);
            return ["({$filterA} || {$filterB})", array_merge($argsA, $argsB), $argsCount];
        }

        switch (get_class($filter)) {
            case DocumentStore\Filter\EqFilter::class:
                /** @var DocumentStore\Filter\EqFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop == @a$argsCount", ["a$argsCount" => $filter->val()], ++$argsCount];
            case DocumentStore\Filter\GtFilter::class:
                /** @var DocumentStore\Filter\GtFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop > @a$argsCount", ["a$argsCount" => $filter->val()], ++$argsCount];
            case DocumentStore\Filter\GteFilter::class:
                /** @var DocumentStore\Filter\GteFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop >= @a$argsCount", ["a$argsCount" => $filter->val()], ++$argsCount];
            case DocumentStore\Filter\LtFilter::class:
                /** @var DocumentStore\Filter\LtFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop < @a$argsCount", ["a$argsCount" => $filter->val()], ++$argsCount];
            case DocumentStore\Filter\LteFilter::class:
                /** @var DocumentStore\Filter\LteFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop <= @a$argsCount", ["a$argsCount" => $filter->val()], ++$argsCount];
            case DocumentStore\Filter\LikeFilter::class:
                // @todo
                return [];
            case DocumentStore\Filter\NotFilter::class:
                // @todo
                return [];
            case DocumentStore\Filter\InArrayFilter::class:
                /** @var DocumentStore\Filter\InArrayFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop IN @a$argsCount", ["a$argsCount" => $filter->val()], ++$argsCount];
            case DocumentStore\Filter\ExistsFilter::class:
                // @todo
                return [];
            default:
                throw new \RuntimeException('Unsupported filter type. Got ' . get_class($filter));
        }
    }

    private function propToJsonPath(string $field): string
    {
        return "d.doc.{$field}";
    }
}
