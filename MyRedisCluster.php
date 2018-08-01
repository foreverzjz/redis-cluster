<?php
/**
 * Created by PhpStorm.
 * User: foreverzjz
 * Date: 2018/7/31
 * Time: 下午4:56
 */

namespace Core\Tools;


class MyRedisCluster extends \RedisCluster
{
    const KEY_NOT_FOUNT = NULL;

    const DEFAULT_TTL = 10800;
    const FOREVER_TTL = -1;
    const TODAY_TTL = 0;

    public function setExpire($key, $ttl = 0)
    {
        if ($ttl == self::TODAY_TTL) {
            $ttl = strtotime(date('Y-m-d 23:59:59')) - time();
        }
        return parent::expire($key, $ttl);
    }

    public function existsKey($key)
    {
        return parent::exists($key);
    }

    public function get($keyName, $deserialize = FALSE)
    {
        $result = parent::get($keyName);
        if($result !== false){
            if ($deserialize) {
                $result = json_decode($result, TRUE);
            }
        }
        return $result;
    }

    public function mget($keys,$column="",$deserialize=false)
    {
        $cacheInfo = parent::mget($keys);
        $result = array();
        if($deserialize) {
            foreach($cacheInfo as $item) {
                if($item !== false) {
                    $item = json_decode($item,true);
                    if(!empty($column)) {
                        $result[$item[$column]] = $item;
                    } else {
                        $result[] = $item;
                    }
                }
            }
        } else {
            foreach ($cacheInfo as $item) {
                if($item !== false) {
                    $result[] = $item;
                }
            }
        }
        return $result;
    }

    public function mset($result,$ttl = -1)
    {
        foreach($result as $key => $value) {
            if (is_array($value) || is_object($value)) {
                $result[$key] = json_encode($value);
            }
        }
        if($ttl < 0) {
            parent::mset($result);
        } else {
            if ($ttl == self::TODAY_TTL) {
                $ttl = strtotime(date('Y-m-d 23:59:59')) - time();
            }
            if ($ttl > 1) {
                foreach($result as $key => $value) {
                    parent::setex($key, $ttl, $value);
                }
            }
        }
    }

    public function set($key, $value, $ttl = -1)
    {
        if (strlen($key) && ($value !== NULL)) {
            if (is_array($value) || is_object($value)) {
                $value = json_encode($value);
            }
            if ($ttl < 0) {
                parent::set($key, $value);
            } else {
                if ($ttl == self::TODAY_TTL) {
                    $ttl = strtotime(date('Y-m-d 23:59:59')) - time();
                }
                if ($ttl > 1) {
                    parent::setex($key, $ttl, $value);
                }
            }
        }
    }

    public function incr($key)
    {
        $incrResult = parent::incr($key);
        return $incrResult;
    }

    public function decr($key)
    {
        return parent::decr($key);
    }

    public function incrBy($key, $incr,$ttl = -1)
    {
        $incrResult = parent::incrBy($key, $incr);
        if ($incrResult) {
            if ($ttl == 0) {
                parent::expire($key, strtotime(date('Y-m-d 23:59:59')) - time());
            } elseif ($ttl > 0) {
                parent::expire($key, $ttl);
            }
        }
        return $incrResult;
    }

    public function decrBy($key, $incr,$ttl = -1)
    {
        $decrResult = parent::decrBy($key, $incr);
        if ($decrResult) {
            if ($ttl == 0) {
                parent::expire($key, strtotime(date('Y-m-d 23:59:59')) - time());
            } elseif ($ttl > 0) {
                parent::expire($key, $ttl);
            }
        }
        return $decrResult;
    }

    public function setIncr($key, $ttl = -1)
    {
        $incrResult = parent::incr($key);
        if ($incrResult) {
            if ($ttl == 0) {
                parent::expire($key, strtotime(date('Y-m-d 23:59:59')) - time());
            } elseif ($ttl > 0) {
                parent::expire($key, $ttl);
            }
        }
        return $incrResult;
    }

    public function setDecr($key, $ttl = -1)
    {
        $decrResult = parent::decr($key);
        if ($decrResult) {
            if ($ttl == 0) {
                parent::expire($key, strtotime(date('Y-m-d 23:59:59')) - time());
            } elseif ($ttl > 0) {
                parent::expire($key, $ttl);
            }
        }
        return $decrResult;
    }

    public function delete($key)
    {
        return parent::del($key);
    }

    public function hashLength($key)
    {
        return parent::hLen($key);
    }

    public function hashGet($key, $hashKey, $unSerialize = FALSE)
    {
        $val = parent::hGet($key, $hashKey);
        if ($unSerialize) {
            $val = json_decode($val, TRUE);
        }
        return $val;
    }

    public function hashIncrBy($key, $hashKey, $incr = 1)
    {
        return parent::hIncrBy($key, $hashKey, $incr);
    }

    public function hashSet($key, $hashKey, $value)
    {
        if (is_array($value) || is_object($value)) {
            $value = json_encode($value, JSON_UNESCAPED_UNICODE);
        }
        return parent::hSet($key, $hashKey, $value);
    }

    public function hashMultiSet($key, $map, $ttl = -1)
    {
        $mapVal = [];
        foreach ($map as $hashKey => $value) {
            if (is_array($value) || is_object($value)) {
                $value = json_encode($value, JSON_UNESCAPED_UNICODE);
            }
            $mapVal[$hashKey] = $value;
        }
        $result = parent::hMSet($key, $mapVal);
        if ($result) {
            if ($ttl == 0) {
                parent::expire($key, strtotime(date('Y-m-d 23:59:59')) - time());
            } elseif ($ttl > 0) {
                parent::expire($key, $ttl);
            }
        }
        return $result;
    }


    public function hashMultiGet($key, $fields, $unserialize = FALSE)
    {
        $resultSet = parent::hMGet($key, $fields);
        if (!$resultSet) {
            return $resultSet;
        }
        if ($unserialize) {
            $newResultSet = [];
            foreach ($resultSet as $key => $row) {
                $newResultSet[$key] = json_decode($row, JSON_UNESCAPED_UNICODE);
            }
            return $newResultSet;
        }
        return $resultSet;
    }

    public function hashGetAll($key, $unSerialize = FALSE)
    {
        $resultSet = parent::hGetAll($key);
        if (!$resultSet) {
            return $resultSet;
        }
        return $resultSet;
    }

    public function hashDestroyField($key, $field)
    {
        if (is_array($field)) {
            array_unshift($field, $key);
            return call_user_func_array(array($this, 'hDel'), $field);
        } else {
            return parent::hDel($key, $field);
        }
    }

    public function zAdd($key, $score, $value)
    {
        return parent::zAdd($key, $score, $value);
    }

    public function zGetAll($key)
    {
        return parent::zRange($key, 0, -1);
    }

    public function zRevRangeByScore($key, $max, $min, $options)
    {
        return parent::zRevRangeByScore($key, $max, $min, $options);
    }

    public function zRangeByScore($key, $min, $max, $options)
    {
        return parent::zRangeByScore($key, $min, $max, $options);
    }

    public function zRemove($key, $member)
    {
        return parent::zRem($key, $member);
    }

    public function zIncrBy($key, $member, $score)
    {
        return parent::zIncrBy($key, $score, $member);
    }

    public function zInter($key, $subKeys)
    {
        $weights = [1];
        $weights = array_pad($weights, count($subKeys), 0);
        return parent::zInterStore($key, $subKeys, $weights);
    }

    public function zUnion($key, $subKeys)
    {
        return parent::zUnionStore($key, $subKeys);
    }

    public function zScore($key, $member)
    {
        return parent::zScore($key, $member);
    }

    /**
     * zSize
     * @param $key
     * @return integer
     */
    public function zSize($key)
    {
        return parent::zCard($key);
    }

    public function destroy($key)
    {
        return parent::del($key);
    }

    /**
     * 获取锁
     * @param  String  $key    锁标识
     * @param  Int     $expire 锁过期时间
     * @return Boolean
     */
    public function lock($key, $expire = 5)
    {
        $is_lock = parent::setnx($key, time()+$expire);

        // 不能获取锁
        if(!$is_lock){

            // 判断锁是否过期
            $lock_time = parent::get($key);

            // 锁已过期，删除锁，重新获取
            if(time()>$lock_time){
                $this->unlock($key);
                $is_lock = parent::setnx($key, time()+$expire);
            }
        }

        return $is_lock? true : false;
    }

    /**
     * 释放锁
     * @param  String  $key 锁标识
     * @return Boolean
     */
    public function unlock($key)
    {
        return parent::del($key);
    }
}