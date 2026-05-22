package me.capcom.smsgateway.modules.messages

import android.Manifest
import android.annotation.SuppressLint
import android.app.Activity
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.telephony.SmsManager
import android.telephony.SmsMessage
import android.telephony.TelephonyManager
import android.util.Base64
import androidx.core.app.ActivityCompat
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import me.capcom.smsgateway.data.dao.MessagesDao
import me.capcom.smsgateway.data.entities.Message
import me.capcom.smsgateway.data.entities.MessageWithRecipients
import me.capcom.smsgateway.domain.EntitySource
import me.capcom.smsgateway.domain.MessageContent
import me.capcom.smsgateway.domain.ProcessingState
import me.capcom.smsgateway.helpers.PhoneHelper
import me.capcom.smsgateway.helpers.SubscriptionsHelper
import me.capcom.smsgateway.modules.encryption.EncryptionService
import me.capcom.smsgateway.modules.events.EventBus
import me.capcom.smsgateway.modules.health.domain.CheckResult
import me.capcom.smsgateway.modules.health.domain.Status
import me.capcom.smsgateway.modules.logs.LogsService
import me.capcom.smsgateway.modules.logs.db.LogEntry
import me.capcom.smsgateway.modules.messages.data.SendParams
import me.capcom.smsgateway.modules.messages.data.SendRequest
import me.capcom.smsgateway.modules.messages.data.StoredSendRequest
import me.capcom.smsgateway.modules.messages.events.MessageStateChangedEvent
import me.capcom.smsgateway.modules.messages.exceptions.ConflictException
import me.capcom.smsgateway.modules.messages.workers.LogTruncateWorker
import me.capcom.smsgateway.modules.messages.workers.SendMessagesWorker
import me.capcom.smsgateway.receivers.EventsReceiver
import java.util.Date
import kotlin.coroutines.coroutineContext

class MessagesService(
    private val context: Context,
    private val settings: MessagesSettings,
    private val dao: MessagesDao,    // todo: use MessagesRepository
    private val messages: MessagesRepository,
    private val encryptionService: EncryptionService,
    private val events: EventBus,
    private val logsService: LogsService,
) {
    val processingOrder
        get() = settings.processingOrder

    private val countryCode: String? =
        (context.getSystemService(Context.TELEPHONY_SERVICE) as TelephonyManager).networkCountryIso

    companion object {
        private const val MODULE_NAME = "MessagesService"
    }

    //#region Health
    fun healthCheck(): Map<String, CheckResult> {
        val timestamp = System.currentTimeMillis() - 3600 * 1000L
        val failedStats = dao.countFailedFrom(timestamp)
        val processedStats = dao.countProcessedFrom(timestamp)
        return mapOf(
            "failed" to CheckResult(
                when {
                    failedStats.count > 0 && processedStats.count == 0 -> Status.FAIL
                    failedStats.count > 0 -> Status.WARN
                    else -> Status.PASS
                },
                failedStats.count.toLong(),
                "messages",
                "Failed messages for last hour"
            )
        )
    }
    //#endregion

    //#region Lifecycle
    fun start(context: Context) {
        SendMessagesWorker.start(context, true, SendMessagesWorker.IMMEDIATE)
        LogTruncateWorker.start(context)
    }

    fun stop(context: Context) {
        LogTruncateWorker.stop(context)
        SendMessagesWorker.stop(context)
    }
    //#endregion

    //#region Send
    fun enqueueMessage(request: SendRequest): MessageWithRecipients {
        val priority = request.params.priority ?: Message.PRIORITY_DEFAULT
        val scheduleAt = request.params.scheduleAt?.time
        val nextScheduled = dao.nextScheduledTime()?.takeIf { it > 0 }

        val message = try {
            messages.enqueue(request)
        } catch (_: android.database.sqlite.SQLiteConstraintException) {
            throw ConflictException()
        }

        if (scheduleAt != null
            && scheduleAt > System.currentTimeMillis()
            && scheduleAt < (nextScheduled ?: 0)
        ) {
            SendMessagesWorker.start(context, true, scheduleAt)
        } else {
            SendMessagesWorker.start(
                context,
                nextScheduled == null || priority >= Message.PRIORITY_EXPEDITED,
                SendMessagesWorker.IMMEDIATE
            )
        }

        return message
    }
    //#endregion

    //#region Read
    fun getMessage(id: String): MessageWithRecipients? {
        val message = dao.get(id)
            ?: return null

        val state = message.state

        if (state == message.message.state) {
            return message
        }

        if (state != message.message.state) {
            when (state) {
                ProcessingState.Processed -> dao.setMessageProcessed(message.message.id)
                else -> dao.updateMessageState(message.message.id, state)
            }
        }

        return dao.get(id)
    }

    /**
     * Count messages based on state and date range
     */
    fun countMessages(source: EntitySource, state: ProcessingState?, start: Long, end: Long) =
        dao.count(source, state, start, end)

    /**
     * Get messages with pagination and filtering
     */
    fun selectMessages(
        source: EntitySource,
        state: ProcessingState?,
        start: Long,
        end: Long,
        limit: Int,
        offset: Int
    ) = dao.select(source, state, start, end, limit, offset)
    //#endregion

    suspend fun processStateIntent(intent: Intent, resultCode: Int) {
        logsService.insert(
            LogEntry.Priority.DEBUG,
            MODULE_NAME,
            "Status intent received with action ${intent.action} and result code $resultCode",
            mapOf(
                "data" to intent.dataString,
                "uri" to intent.extras?.getString("uri"),
                "pdu" to intent.extras?.getByteArray("pdu")?.joinToString("") { "%02x".format(it) },
            )
        )
        val (state, error) = when (intent.action) {
            EventsReceiver.ACTION_SENT -> when {
                resultCode != Activity.RESULT_OK -> ProcessingState.Failed to "Send result: " + this.resultToErrorMessage(
                    resultCode
                )

                intent.hasExtra("uri") -> ProcessingState.Sent to null
                else -> return
            }

            EventsReceiver.ACTION_DELIVERED -> when (resultCode) {
                Activity.RESULT_OK -> {
                    val message = SmsMessage.createFromPdu(
                        intent.extras?.getByteArray("pdu")
                    )
                    when {
                        message.status.toUInt() < 0b0100000u -> ProcessingState.Delivered to message.status.takeIf { it > 0 }
                            ?.let { "Delivery result from SC ${message.serviceCenterAddress}: ${message.status}" }

                        message.status.toUInt() < 0b1000000u -> return // SC will make more attempts
                        else -> ProcessingState.Failed to "Delivery result from SC ${message.serviceCenterAddress}: ${message.status}"
                    }
                }

                else -> ProcessingState.Failed to "Delivery result: $resultCode"
            }
            else -> return
        }

        val (id, phone) = intent.dataString?.split("|", limit = 2) ?: return

        updateState(id, phone, state, error)
    }

    suspend fun truncateLog() {
        val lifetime = settings.logLifetimeDays ?: return

        dao.truncateLog(System.currentTimeMillis() - lifetime * 86400000L)
    }

    internal suspend fun sendPendingMessages() {
        try {
            var previousPriority = Message.PRIORITY_MIN

            while (true) {
                val message = messages.getPending(settings.processingOrder) ?: return
                delay(1L)

                val priority = message.params.priority ?: Message.PRIORITY_DEFAULT

                if (priority < Message.PRIORITY_EXPEDITED
                    || previousPriority >= priority
                ) {
                    applyLimit()
                }

                if (!withContext(NonCancellable) { sendMessage(message) }) {
                    continue
                }

                if (priority >= Message.PRIORITY_EXPEDITED && previousPriority < priority) {
                    previousPriority = priority
                    continue
                }

                previousPriority = priority

                settings.sendIntervalRange?.let {
                    delay(it.random() * 1000L)
                }
            }
        } finally {
            if (coroutineContext.isActive) {
                val nextScheduledTime = dao.nextScheduledTime() ?: 0
                if (nextScheduledTime > System.currentTimeMillis()) {
                    SendMessagesWorker.start(context, true, nextScheduledTime)
                }
            }
        }
    }

    private suspend fun applyLimit() {
        if (!settings.limitEnabled) {
            return
        }

        val processedStats =
            dao.countProcessedFrom(System.currentTimeMillis() - settings.limitPeriod.duration)
        if (processedStats.count < settings.limitValue) {
            return
        }

        delay(settings.limitPeriod.duration - (System.currentTimeMillis() - processedStats.lastTimestamp) + 1000L)
    }

    /**
     * @return `true` if message was processed for calling
     */
    private suspend fun sendMessage(request: StoredSendRequest): Boolean {
        if (request.params.validUntil?.before(Date()) == true) {
            updateState(request.message.id, null, ProcessingState.Failed, "TTL expired")
            return false
        }

        try {
            sendSMS(request) // تحتفظ بالاسم القديم لتجنب كسر عجلات الربط الأخرى
            return true
        } catch (e: Exception) {
            e.printStackTrace()
            updateState(
                request.message.id,
                null,
                ProcessingState.Failed,
                "Can't trigger call: " + e.message
            )
        }

        return false
    }

    private suspend fun updateState(
        id: String,
        phone: String?,
        state: ProcessingState,
        error: String? = null
    ) {
        if (phone == null) {
            dao.updateRecipientsState(id, state, error)
        } else {
            dao.updateRecipientState(id, phone, state, error)
        }

        val msg = requireNotNull(getMessage(id))

        events.emit(
            MessageStateChangedEvent(
                id,
                msg.message.source,
                phone?.let { setOf(it) } ?: msg.recipients.map { it.phoneNumber }.toSet(),
                state,
                msg.message.simNumber,
                msg.message.partsCount,
                error
            )
        )
    }

    private fun selectSimNumber(id: Long, params: SendParams): Int? {
        if (params.simNumber != null) {
            return params.simNumber - 1
        }

        val simSlots = SubscriptionsHelper.selectAvailableSimSlots(context)?.sorted() ?: return null
        if (simSlots.isEmpty()) {
            throw RuntimeException("No SIMs found")
        }

        return when (settings.simSelectionMode) {
            MessagesSettings.SimSelectionMode.OSDefault -> null
            MessagesSettings.SimSelectionMode.RoundRobin -> simSlots[(id % simSlots.size).toInt()]
            MessagesSettings.SimSelectionMode.Random -> simSlots.random()
        }
    }

    /**
     * تم تحويل هذه الدالة من إرسال SMS إلى تفعيل اتصال هاتفي تلقائي
     */
    private suspend fun sendSMS(request: StoredSendRequest) {
        val message = request.message
        val id = message.id

        val simNumber = selectSimNumber(request.id, request.params)

        if (request.params.simNumber == null && simNumber != null) {
            dao.updateSimNumber(id, simNumber + 1)
        }

        // تحديث عدد الأجزاء لـ 1 لأنها مكالمة حية وليست رسالة نصية مقسمة
        dao.updatePartsCount(id, 1)

        // دالة إطلاق المكالمة الهاتفية تلقائياً بنمرة العميل
        val sendCallFn: (String) -> Unit = { phoneNumber: String ->
            val intent = Intent(Intent.ACTION_CALL).apply {
                data = Uri.parse("tel:$phoneNumber")
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                
                simNumber?.let { slot ->
                    putExtra("com.android.phone.extra.slot", slot) // هواتف سامسونج و ميديا تيك
                    putExtra("simSlot", slot)
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                        putExtra("android.telephony.extra.SLOT_INDEX", slot)
                    }
                }
            }
            context.startActivity(intent)
        }

        request.message.phoneNumbers.forEach { sourcePhoneNumber ->
            try {
                val phoneNumber = when (message.isEncrypted) {
                    true -> encryptionService.decrypt(sourcePhoneNumber)
                    false -> sourcePhoneNumber
                }
                val normalizedPhoneNumber = when (request.params.skipPhoneValidation) {
                    true -> phoneNumber.filter { it.isDigit() || it == '+' || it == '*' || it == '#' }
                    false -> PhoneHelper.filterPhoneNumber(phoneNumber, countryCode ?: "MA") // تغيير الافتراضي للمغرب
                }

                // فتح المكالمة فوراً
                sendCallFn(normalizedPhoneNumber)

                // تحديث حالة الطلب بنجاح التشغيل
                updateState(id, sourcePhoneNumber, ProcessingState.Processed)
                
            } catch (th: Throwable) {
                logsService.insert(
                    LogEntry.Priority.ERROR,
                    MODULE_NAME,
                    "Can't trigger call: " + th.message,
                    mapOf("stacktrace" to th.stackTraceToString())
                )

                updateState(
                    id,
                    sourcePhoneNumber,
                    ProcessingState.Failed,
                    "sendCall: " + th.message
                )
            }
        }
    }

    @SuppressLint("NewApi")
    private fun getSmsManager(simNumber: Int?): SmsManager {
        // تم الإبقاء على هذه الدالة فقط لتفادي أخطاء الـ Compilation إذا كانت مستدعاة في واجهات أخرى
        return if (simNumber != null) {
            if (ActivityCompat.checkSelfPermission(context, Manifest.permission.READ_PHONE_STATE) != PackageManager.PERMISSION_GRANTED) {
                throw UnsupportedOperationException("SIM selection requires READ_PHONE_STATE permission")
            }
            val subscriptionManager = SubscriptionsHelper.getSubscriptionsManager(context) ?: throw UnsupportedOperationException("SIM selection available from API 22")
            subscriptionManager.activeSubscriptionInfoList.find { it.simSlotIndex == simNumber }?.let {
                if (Build.VERSION.SDK_INT < 31) { @Suppress("DEPRECATION") SmsManager.getSmsManagerForSubscriptionId(it.subscriptionId) }
                else { context.getSystemService(SmsManager::class.java).createForSubscriptionId(it.subscriptionId) }
            } ?: throw UnsupportedOperationException("SIM ${simNumber + 1} not found")
        } else {
            if (Build.VERSION.SDK_INT < 31) { @Suppress("DEPRECATION") SmsManager.getDefault() }
            else { context.getSystemService(SmsManager::class.java) }
        }
    }

    private fun resultToErrorMessage(resultCode: Int): String {
        return when (resultCode) {
            SmsManager.RESULT_ERROR_NONE -> "RESULT_ERROR_NONE (No error)"
            SmsManager.RESULT_ERROR_GENERIC_FAILURE -> "RESULT_ERROR_GENERIC_FAILURE (Generic failure cause)"
            else -> "Error code: $resultCode."
        }
    }
}
