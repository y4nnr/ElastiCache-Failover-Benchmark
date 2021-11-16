package elasticache.test

import org.redisson.connection.CRC16
import org.redisson.connection.MasterSlaveConnectionManager
import java.lang.IllegalStateException

fun calcHashSlotSlot(key: String): Int {
    val start = key.indexOf('{')
    var k = key
    if (start != -1) {
        val end = key.indexOf('}')
        if (end != -1 && start + 1 < end) {
            k = key.substring(start + 1, end)
        }
    }
    return CRC16.crc16(k.toByteArray()) % MasterSlaveConnectionManager.MAX_SLOT
}

fun findKeyForSlot(requiredSlot: Int): String {
    var i = 0
    var hashSlot = 0
    do {
        hashSlot = calcHashSlotSlot(i.toString())
        i++
        if (i > 1000000) {
            throw IllegalStateException("We're finding hashslot sooo long")
        }
    } while (requiredSlot != hashSlot)
    return i.toString()
}