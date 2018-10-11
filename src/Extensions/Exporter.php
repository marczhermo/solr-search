<?php

namespace Marcz\Solr\Extensions;

use SilverStripe\Core\Extension;
use Marcz\Solr\SolrClient;

class Exporter extends Extension
{
    public function updateExport(&$data, &$clientClassName)
    {
        if ($clientClassName === SolrClient::class) {
            // TODO:
        }
    }
}
