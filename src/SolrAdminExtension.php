<?php

namespace Marcz\Solr;

use SilverStripe\Core\Extension;
use SilverStripe\View\Requirements;

class SolrAdminExtension extends Extension
{
    public function init()
    {
        Requirements::javascript('marczhermo/solr-cloud: client/dist/js/bundle.js');
        Requirements::css('marczhermo/solr-cloud: client/dist/styles/bundle.css');
    }
}
