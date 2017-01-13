<?php

/**
 * Log filename
 */
$_logFile = 'precessExpired.' . date('Ymd_His') . '.log';

/**
 * Print and log function
 *
 * @param string $string
 */
function _p($string = '')
{
    global $_logFile;
    echo $string . "\n";
    Mage::log($string, null, $_logFile, true);
}

/**
 * Returns user-friendly formatted time
 *
 * @param $time
 * @return string
 */
function _formatTime($time)
{
    return sprintf('%02d:%02d:%02d', ($time / 3600), ($time / 60 % 60), $time % 60);
}

/**
 * Returns count of Reward History entities left to process
 *
 * @param Enterprise_Reward_Model_Resource_Reward_History $rewardHistoryResource
 * @param Magento_Db_Adapter_Pdo_Mysql $adapter
 * @param $websiteId
 * @param $expiryType
 * @return int
 */
function getCountHistoryToProcess(Enterprise_Reward_Model_Resource_Reward_History $rewardHistoryResource,
    Magento_Db_Adapter_Pdo_Mysql $adapter, $websiteId, $expiryType)
{
    $now = Varien_Date::formatDate(time(), true);
    $field = $expiryType == 'static' ? 'expired_at_static' : 'expired_at_dynamic';

    $select = $adapter->select()
        ->from($rewardHistoryResource->getMainTable(), 'COUNT(*)')
        ->where('website_id = :website_id')
        ->where("{$field} < :time_now")
        ->where("{$field} IS NOT NULL")
        ->where('is_expired=?', 0)
        ->where('points_delta-points_used > ?', 0);
    $bind = array(
        ':website_id' => $websiteId,
        ':time_now'   => $now
    );

    /** @var Varien_Db_Statement_Pdo_Mysql $stmt */
    $stmt = $adapter->query($select, $bind);
    return (int)$stmt->fetchColumn();
}

require_once './app/Mage.php';
Mage::init();

/** @var Mage_Core_Model_Resource $resource */
$resource = Mage::getModel('core/resource');
/** @var Magento_Db_Adapter_Pdo_Mysql $adapter */
$adapter = $resource->getConnection('core_write');
/** @var Enterprise_Reward_Model_Resource_Reward_History $rewardHistoryResource */
$rewardHistoryResource = Mage::getResourceModel('enterprise_reward/reward_history');

/**
 * Loop counter
 */
$loopCounter = 0;

/**
 * Total items counter
 */
$totalItemsCounter = 0;

/**
 * Website items left to process
 */
$websiteItemsCounter = 0;

/**
 * Limit items per batch
 */
$batchLimit = 100;

/**
 * Start microtime
 */
$startMicroTime = microtime(true);

foreach (Mage::app()->getWebsites() as $website) {
    $websiteId = $website->getId();
    $expiryType = Mage::helper('enterprise_reward')->getGeneralConfig('expiry_calculation', $websiteId);
    $count = getCountHistoryToProcess($rewardHistoryResource, $adapter, $websiteId, $expiryType);
    $websiteItemsCounter = $count;
    _p("Starting processing {$websiteItemsCounter} items for website ID = {$websiteId}");
    $totalItemsCounter += $count;
    while ($count > 0) {
        ++$loopCounter;
        $rewardHistoryResource->expirePoints($website->getId(), $expiryType, $batchLimit);
        if ( !($loopCounter % 10) ) {
            _p($loopCounter * $batchLimit
                . "/{$websiteItemsCounter} items were processed successfully, left {$count} items");
        }
        $count = getCountHistoryToProcess($rewardHistoryResource, $adapter, $websiteId, $expiryType);
    }
}

$finishMicroTime = microtime(true);
$executionTime = $finishMicroTime - $startMicroTime;
_p($totalItemsCounter . ' items were processed in ' . _formatTime($executionTime) . " ($executionTime seconds)");
