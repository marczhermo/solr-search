<?php

namespace Marcz\Solr;

use SilverStripe\Core\Injector\Injectable;
use SilverStripe\Core\Environment;
use SilverStripe\Core\Injector\Injector;
use SilverStripe\Core\Config\Configurable;
use Symbiote\QueuedJobs\Services\QueuedJobService;
use Marcz\Solr\Jobs\JsonBulkExport;
use Marcz\Solr\Jobs\JsonExport;
use SilverStripe\ORM\DataList;
use SilverStripe\ORM\ArrayList;
use Marcz\Search\Config as SearchConfig;
use Marcz\Search\Client\SearchClientAdaptor;
use Marcz\Solr\Jobs\DeleteRecord;
use GuzzleHttp\Ring\Client\CurlHandler;
use GuzzleHttp\Stream\Stream;
use GuzzleHttp\Ring\Future\CompletedFutureArray;
use Marcz\Search\Client\DataWriter;
use Marcz\Search\Client\DataSearcher;
use Exception;
use SilverStripe\Dev\Debug;


class SolrClient implements SearchClientAdaptor, DataWriter, DataSearcher
{
    use Injectable, Configurable;

    protected $clientIndex;
    protected $clientIndexName;
    protected $clientAPI;
    protected $response;
    protected $rawQuery;

    private static $batch_length = 100;

    public function createClient()
    {
        if ($this->clientAPI) {
            return $this->clientAPI;
        }

        return $this->setClientAPI(new CurlHandler());
    }

    public function setClientAPI($handler)
    {
        $this->clientAPI = $handler;

        return $this->clientAPI;
    }

    public function initIndex($indexName)
    {
        $this->createClient();

        $this->clientIndexName = $indexName;

        $endPoint = Environment::getEnv('SS_SOLR_END_POINT');
        $this->rawQuery = [
            'http_method'   => 'GET',
            'uri'           => parse_url($endPoint, PHP_URL_PATH),
            'headers'       => [
                'host'  => [parse_url($endPoint, PHP_URL_HOST)],
                'port'  => [parse_url($endPoint, PHP_URL_PORT)],
                'Content-Type' => ['application/json'],
            ],
            'client' => [
                'curl' => [
                    CURLOPT_SSL_VERIFYHOST => 0,
                    CURLOPT_SSL_VERIFYPEER => false,
                    CURLOPT_PORT => parse_url($endPoint, PHP_URL_PORT),
                ]
            ]
        ];

        return $this->sql();
    }

    public function createIndex($indexName)
    {
        return ($this->hasEngine($indexName)) ? true : $this->createEngine($indexName);
    }

    public function hasEngine($indexName)
    {
        $data = ['action' => 'LIST'];
        $url = sprintf(
            '%s/admin/collections?%s',
            parse_url(Environment::getEnv('SS_SOLR_END_POINT'), PHP_URL_PATH),
            http_build_query($data)
        );
        $rawQuery = $this->initIndex($indexName);
        $rawQuery['uri'] = $url;
        $this->rawQuery = $rawQuery;
        $handler = $this->clientAPI;
        $response = $this->checkResponse($handler($rawQuery));

        if (!isset($response['body']['collections'])) {
            throw new Exception('Unable to get collections');
        }

        if (!$response['body']['collections']) {
            return false;
        }

        return in_array(strtolower($indexName), $response['body']['collections']);
    }

    public function createEngine($indexName)
    {
        $indices = ArrayList::create(SearchConfig::config()->get('indices'));
        $index = $indices->find('name', $indexName);

        $data = [
            'action' => 'CREATE',
            'name' => strtolower($indexName),
            'numShards' => (isset($index['numShards'])) ? $index['numShards'] : 2,
        ];

        $rawQuery = $this->initIndex($indexName);

        $url = sprintf(
            '%s/admin/collections?%s',
            parse_url(Environment::getEnv('SS_SOLR_END_POINT'), PHP_URL_PATH),
            http_build_query($data)
        );

        $rawQuery['uri'] = $url;
        $this->rawQuery = $rawQuery;
        $handler = $this->clientAPI;
        $response = $this->checkResponse($handler($rawQuery));

        return $response['status'] === 200;
    }

    public function checkResponse(CompletedFutureArray $response)
    {
        if (is_resource($response['body']) && get_resource_type($response['body']) === 'stream') {
            $stream = Stream::factory($response['body']);
            $response['body'] = $stream->getContents();
        }

        if ($response['status'] === null) {
            if ($response['error'] instanceof Exception) {
                throw $response['error'];
            }
            throw new Exception('Unknown status code');
        }

        $body = json_decode($response['body'], true);
        if ($response['status'] >= 400) {
            $error = sprintf(
                '%s - %s%s',
                $response['status'],
                $response['reason'],
                (isset($body['error']['msg'])) ? ', '. $body['error']['msg'] : ''
            );
            throw new Exception($error);
        }

        $response['body'] = $body;

        return $response;
    }

    public function update($data)
    {
        return $this->bulkUpdate([$data]);
    }

    public function bulkUpdate($list)
    {
        $indexName = strtolower($this->clientIndexName);
        $rawQuery = $this->initIndex($this->clientIndexName);

        $endPoint = Environment::getEnv('SS_SOLR_END_POINT');
        $url = sprintf(
            '%1$s/%2$s/update?commit=true',
            parse_url($endPoint, PHP_URL_PATH),
            $indexName
        );

        $rawQuery['http_method'] = 'POST';
        $rawQuery['uri'] = $url;
        $rawQuery['body'] = json_encode($list, JSON_PRESERVE_ZERO_FRACTION);

        $this->rawQuery = $rawQuery;

        $handler = $this->clientAPI;
        $response = $handler($rawQuery);
        $stream = Stream::factory($response['body']);
        $response['body'] = $stream->getContents();

        return isset($response['status']) && 200 === $response['status'];
    }

    public function deleteRecord($recordID)
    {
        $indexName = strtolower($this->clientIndexName);
        $rawQuery = $this->initIndex($this->clientIndexName);

        $endPoint = Environment::getEnv('SS_SOLR_END_POINT');
        $url = sprintf(
            '%1$s/%2$s/update?commit=true',
            parse_url($endPoint, PHP_URL_PATH),
            $indexName
        );

        $rawQuery['http_method'] = 'POST';
        $rawQuery['uri'] = $url;
        $rawQuery['body'] = json_encode(['delete' => [$recordID]], JSON_PRESERVE_ZERO_FRACTION);

        $this->rawQuery = $rawQuery;

        $handler = $this->clientAPI;
        $response = $handler($rawQuery);
        $stream = Stream::factory($response['body']);
        $response['body'] = $stream->getContents();

        return isset($response['status']) && 200 === $response['status'];
    }

    public function createBulkExportJob($indexName, $className)
    {
        $list        = new DataList($className);
        $total       = $list->count();
        $batchLength = self::config()->get('batch_length') ?: SearchConfig::config()->get('batch_length');
        $totalPages  = ceil($total / $batchLength);

        $this->initIndex($indexName);

        for ($offset = 0; $offset < $totalPages; $offset++) {
            $job = Injector::inst()->createWithArgs(
                JsonBulkExport::class,
                [$indexName, $className, $offset * $batchLength]
            );

            singleton(QueuedJobService::class)->queueJob($job);
        }
    }

    public function createExportJob($indexName, $className, $recordId)
    {
        $job = Injector::inst()->createWithArgs(
            JsonExport::class,
            [$indexName, $className, $recordId]
        );

        singleton(QueuedJobService::class)->queueJob($job);
    }

    public function createDeleteJob($indexName, $className, $recordId)
    {
        $job = Injector::inst()->createWithArgs(
            DeleteRecord::class,
            [$indexName, $className, $recordId]
        );

        singleton(QueuedJobService::class)->queueJob($job);
    }

    public function search($term = '', $filters = [], $pageNumber = 0, $pageLength = 20)
    {
        $term = trim($term);
        $indexName = strtolower($this->clientIndexName);
        $this->rawQuery = $this->initIndex($this->clientIndexName);

        $endPoint = Environment::getEnv('SS_SOLR_END_POINT');
        $url = sprintf(
                '%1$s/%2$s/query',
            parse_url($endPoint, PHP_URL_PATH),
            $indexName
        );

        $data = [
            'query'  => $term,
            'params' => [
                'debug'  => true,
            ],
            //'offset' => 1 + $pageNumber,
            //'limit'  => $pageLength,
            //'filter' => [$indexName => $this->translateFilterModifiers($filters)],
            //'facet'  => [$indexName => []],
        ];

        $indexConfig = ArrayList::create(SearchConfig::indices())
            ->find('name', $this->clientIndexName);

        if (!empty($indexConfig['attributesForFaceting'])) {
            //$data['facet'] = [$indexName => $indexConfig['attributesForFaceting']];
        }

        $this->rawQuery['uri'] = $url;
        $this->rawQuery['body'] = json_encode($data, JSON_PRESERVE_ZERO_FRACTION);

        $handler = $this->clientAPI;
        $response = $handler($this->rawQuery);
        $stream = Stream::factory($response['body']);
        $response['body'] = $stream->getContents();

        $this->response = json_decode($response['body'], true);
        Debug::dump($this->response);
        //$this->response['_total'] = $this->response['record_count'];

        //return new ArrayList($this->response['records'][$indexName]);
    }

    public function getResponse()
    {
        return $this->response;
    }

    /**
     * Modifies filters
     * @todo Refactor when unit tests is in place.
     * @param array $filters
     * @return array
     */
    public function translateFilterModifiers($filters = [])
    {
        $query       = [];
        $forFilters  = [];
        $forFacets   = [];

        foreach ($filters as $filterArray) {
            foreach ($filterArray as $key => $value) {
                $hasModifier = strpos($key, ':') !== false;
                if ($hasModifier) {
                    $forFilters[][$key] = $value;
                } else {
                    $forFacets[][$key] = $value;
                }
            }
        }

        if ($forFilters) {
            $modifiedFilter   = [];

            foreach ($forFilters as $filterArray) {
                foreach ($filterArray as $key => $value) {
                    $fieldArgs = explode(':', $key);
                    $fieldName = array_shift($fieldArgs);
                    $modifier  = array_shift($fieldArgs);
                    if (is_array($value)) {
                        $modifiedFilter = array_merge(
                            $modifiedFilter,
                            $this->modifyFilters($modifier, $fieldName, $value)
                        );
                    } else {
                        $modifiedFilter[] = $this->modifyFilter($modifier, $fieldName, $value);
                    }
                }
            }

            foreach ($modifiedFilter as $filter) {
                $column = key($filter);
                $previous = isset($query[$column]) ? $query[$column] : [];
                $query[$column] = array_merge($previous, current($filter));
            }
        }

        if ($forFacets) {
            foreach ($forFacets as $filterArray) {
                foreach ($filterArray as $key => $value) {
                    if (is_array($value)) {
                        $query[$key] = array_values($value);
                    } else {
                        $query[$key] = $value;
                    }
                }
            }
        }

        return $query;
    }

    public function callIndexMethod($methodName, $parameters = [])
    {
        return call_user_func_array([$this->clientIndex, $methodName], $parameters);
    }

    public function callClientMethod($methodName, $parameters = [])
    {
        return call_user_func_array([$this->clientAPI, $methodName], $parameters);
    }

    public function modifyFilter($modifier, $key, $value)
    {
        return Injector::inst()->create('Marcz\\SOLR\\Modifiers\\' . $modifier)->apply($key, $value);
    }

    public function modifyFilters($modifier, $key, $values)
    {
        $modifiedFilter = [];

        foreach ($values as $value) {
            $modifiedFilter[] = $this->modifyFilter($modifier, $key, $value);
        }

        return $modifiedFilter;
    }

    public function sql()
    {
        return $this->rawQuery;
    }
}
