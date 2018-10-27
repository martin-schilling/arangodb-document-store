<?php

declare(strict_types=1);

namespace AppTest\Infrastructure\DocumentStore;

use App\Infrastructure\DocumentStore\ArangoDbDocumentStore;
use ArangoDBClient\Connection;
use ArangoDBClient\ConnectionOptions;
use ArangoDBClient\UpdatePolicy;
use PHPUnit\Framework\TestCase;

class ArangoDbDocumentStoreTest extends TestCase
{
    /**
     * @var ArangoDbDocumentStore
     */
    private $documentStore;

    /**
     * @var Connection
     */
    private $connection;

    protected function setUp()
    {
        $this->connection = new Connection([
            ConnectionOptions::OPTION_ENDPOINT => 'tcp://arangodb:8529',
            ConnectionOptions::OPTION_DATABASE => '_system',
            ConnectionOptions::OPTION_AUTH_TYPE => 'Basic',
            ConnectionOptions::OPTION_AUTH_USER => 'root',
            ConnectionOptions::OPTION_AUTH_PASSWD => '',
            ConnectionOptions::OPTION_CONNECTION => 'Keep-Alive',
            ConnectionOptions::OPTION_TIMEOUT => 3,
            ConnectionOptions::OPTION_RECONNECT => true,
            ConnectionOptions::OPTION_CREATE => true,
            ConnectionOptions::OPTION_UPDATE_POLICY => UpdatePolicy::LAST,
        ]);

        $this->documentStore = new ArangoDbDocumentStore($this->connection);

        $this->createCollection('em_ds_test1');
        $this->createCollection('em_test2');
        $this->createCollection('em_ds_test3');
        $this->createCollection('em_ds_xyz');
    }

    protected function tearDown()
    {
        $this->dropCollection('em_ds_test1');
        $this->dropCollection('em_test2');
        $this->dropCollection('em_ds_test3');
        $this->dropCollection('em_ds_xyz');
    }

    protected function createCollection(string $collectionName): void
    {
        $this->connection->post('/_api/collection', \json_encode([
            'name' => $collectionName
        ]));
    }

    protected function dropCollection(string $collectionName): void
    {
        $this->connection->delete('/_api/collection/' . $collectionName);
    }

    protected function hasCollection(string $collectionName): bool
    {
        $query = <<<AQL
FOR collection IN COLLECTIONS() 
    FILTER collection.name == @collectionName
    RETURN collection.name
AQL;

        $result = $this->connection->post('/_api/cursor', \json_encode([
            'query' => $query,
            'bindVars' => [
                'collectionName' => $collectionName
            ]
        ]));

        $result = \json_decode($result->getBody(), true);

        return count($result['result']) === 1;
    }

    protected function getDocument(string $collectionName, string $docId)
    {
        $result = $this->connection->get("/_api/document/{$collectionName}/{$docId}");
        return \json_decode($result->getBody(), true);
    }

    /**
     * @test
     */
    public function it_lists_collections()
    {
        $collections = $this->documentStore->listCollections();

        $this->assertCount(3, $collections);
        $this->assertSame('test1', $collections[0]);
        $this->assertSame('test3', $collections[1]);
        $this->assertSame('xyz', $collections[2]);
    }

    /**
     * @test
     */
    public function it_filters_collections_by_prefix()
    {
        $collections = $this->documentStore->filterCollectionsByPrefix('test');

        $this->assertCount(2, $collections);
        $this->assertSame('test1', $collections[0]);
        $this->assertSame('test3', $collections[1]);
    }

    /**
     * @test
     */
    public function it_checks_has_collection()
    {
        $hasCollection = $this->documentStore->hasCollection('test1');
        $this->assertTrue($hasCollection);

        $hasCollection = $this->documentStore->hasCollection('test2');
        $this->assertFalse($hasCollection);

        $hasCollection = $this->documentStore->hasCollection('something_else');
        $this->assertFalse($hasCollection);
    }

    /**
     * @test
     */
    public function it_adds_collection()
    {
        $this->documentStore->addCollection('my_test_2');
        $this->assertTrue($this->hasCollection('em_ds_my_test_2'));
    }

    /**
     * @test
     */
    public function it_drops_collection()
    {
        $this->assertTrue($this->hasCollection('em_ds_my_test_2'));
        $this->documentStore->dropCollection('my_test_2');
        $this->assertFalse($this->hasCollection('em_ds_my_test_2'));
    }

    /**
     * @test
     */
    public function it_adds_document()
    {
        $this->documentStore->addDoc('test1', 'abc123', ['test' => ['document']]);
        $doc = $this->getDocument('em_ds_test1', 'abc123');

        $this->assertSame('abc123', $doc['_key']);
        $this->assertSame('em_ds_test1/abc123', $doc['_id']);
        $this->assertSame(['test' => ['document']], $doc['doc']);
    }

    /**
     * @test
     */
    public function it_updates_document()
    {
        $this->documentStore->addDoc('test1', 'abc1234', ['test' => ['document']]);

        $doc = $this->getDocument('em_ds_test1', 'abc1234');
        $this->assertSame(['test' => ['document']], $doc['doc']);

        $this->documentStore->updateDoc('test1', 'abc1234', ['test' => ['document', 123]]);

        $doc = $this->getDocument('em_ds_test1', 'abc1234');
        $this->assertSame(['test' => ['document', 123]], $doc['doc']);

    }
}
