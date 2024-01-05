/*
 * Copyright (c) 2014, Texas Instruments Incorporated - http://www.ti.com/
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */
/*---------------------------------------------------------------------------*/
/**
 *
 * Demonstrates MQTT functionality using a local Mosquitto borker.
 * Published messages include a fake temperature reading.
 * @{
 *
 * \file
 * An MQTT example
 */
/*---------------------------------------------------------------------------*/
#include "contiki.h"
#include "mqtt.h"
#include "rpl.h"
#include "net/ipv6/uip.h"
#include "net/ipv6/sicslowpan.h"
#include "sys/etimer.h"
#include "sys/ctimer.h"
#include "leds.h"

#include "sys/log.h"

#define LOG_MODULE "MQTT-MOTE"
#define LOG_LEVEL LOG_LEVEL_INFO

#include <string.h>
/*---------------------------------------------------------------------------*/
/*
 * Publish to a local MQTT broker (e.g. mosquitto) running on
 * the node that hosts your border router
 */
static const char *broker_ip = BROKER_IP_ADDR;
#define DEFAULT_ORG_ID              "mtdssens"
/*---------------------------------------------------------------------------*/
/*
 * A timeout used when waiting for something to happen (e.g. to connect or to
 * disconnect)
 */
#define STATE_MACHINE_PERIODIC     (CLOCK_SECOND >> 1)
/*---------------------------------------------------------------------------*/
/* Provide visible feedback via LEDS during various states */
/* When connecting to broker */
#define CONNECTING_LED_DURATION    (CLOCK_SECOND >> 2)

/* Each time we try to publish */
#define PUBLISH_LED_ON_DURATION    (CLOCK_SECOND)
/*---------------------------------------------------------------------------*/
/* Connections and reconnections */
#define RETRY_FOREVER              0xFF
#define RECONNECT_INTERVAL         (CLOCK_SECOND * 2)
/*---------------------------------------------------------------------------*/
/*
 * Number of times to try reconnecting to the broker.
 * Can be a limited number (e.g. 3, 10 etc) or can be set to RETRY_FOREVER
 */
#define RECONNECT_ATTEMPTS         RETRY_FOREVER
#define CONNECTION_STABLE_TIME     (CLOCK_SECOND * 5)
static struct timer connection_life;
static uint8_t connect_attempt;
/*---------------------------------------------------------------------------*/
/* Various states */
static uint8_t state;
#define STATE_INIT            0
#define STATE_REGISTERED      1
#define STATE_CONNECTING      2
#define STATE_CONNECTED       3
#define STATE_PUBLISHING_CONF 4
#define STATE_LISTENING       5
#define STATE_PUBLISHING      6
#define STATE_DISCONNECTED    7
#define STATE_NEWCONFIG       8
#define STATE_CONFIG_ERROR 0xFE
#define STATE_ERROR        0xFF
/*---------------------------------------------------------------------------*/
#define CONFIG_ORG_ID_LEN        32
#define CONFIG_TYPE_ID_LEN       32
#define CONFIG_AUTH_TOKEN_LEN    32
#define CONFIG_CMD_TYPE_LEN       8
#define CONFIG_IP_ADDR_STR_LEN   64
/*---------------------------------------------------------------------------*/
/* A timeout used when waiting to connect to a network */
#define NET_CONNECT_PERIODIC        (CLOCK_SECOND >> 2)
#define NO_NET_LED_DURATION         (NET_CONNECT_PERIODIC >> 1)
/*---------------------------------------------------------------------------*/
/* Default configuration values */
#define DEFAULT_TYPE_ID             "native"
#define DEFAULT_AUTH_TOKEN          "AUTHZ"
#define DEFAULT_SUBSCRIBE_CMD_TYPE  "+"
#define DEFAULT_BROKER_PORT         1883
#define DEFAULT_PUBLISH_INTERVAL    (60 * CLOCK_SECOND)
#define DEFAULT_KEEP_ALIVE_TIMER    60
/*---------------------------------------------------------------------------*/
PROCESS_NAME(mqtt_mote_process);
AUTOSTART_PROCESSES(&mqtt_mote_process);
/*---------------------------------------------------------------------------*/
/**
 * \brief Data structure declaration for the MQTT client configuration
 */
typedef struct mqtt_client_config {
    char org_id[CONFIG_ORG_ID_LEN];
    char type_id[CONFIG_TYPE_ID_LEN];
    char auth_token[CONFIG_AUTH_TOKEN_LEN];
    char broker_ip[CONFIG_IP_ADDR_STR_LEN];
    char cmd_type[CONFIG_CMD_TYPE_LEN];
    clock_time_t pub_interval;
    uint16_t broker_port;
} mqtt_client_config_t;
/*---------------------------------------------------------------------------*/
/* Maximum TCP segment size for outgoing segments of our socket */
#define MAX_TCP_SEGMENT_SIZE    32
/*---------------------------------------------------------------------------*/
/*
 * Buffers for Client ID and Topic.
 * Make sure they are large enough to hold the entire respective string
 *
 * We also need space for the null termination
 */
#define BUFFER_SIZE 64
static char client_id[BUFFER_SIZE];
static char client_py_id[BUFFER_SIZE];
static char pub_topic[BUFFER_SIZE];
static char sub_topic[BUFFER_SIZE];
static char location_topic[BUFFER_SIZE];
static char value_temp[6];
static char value_hum[6];
/*---------------------------------------------------------------------------*/
/*
 * The main MQTT buffers.
 * We will need to increase if we start publishing more data.
 */
#define APP_BUFFER_SIZE 512
static struct mqtt_connection conn;
static char app_buffer[APP_BUFFER_SIZE];
/*---------------------------------------------------------------------------*/
static struct mqtt_message *msg_ptr = 0;
static struct etimer publish_periodic_timer;
static struct ctimer ct;
static char *buf_ptr;
static uint16_t seq_nr_value = 0;
/*---------------------------------------------------------------------------*/
//To start to publish reeal sensor data
static int id_not_yet_set = 1;
/*---------------------------------------------------------------------------*/
static mqtt_client_config_t conf;
/*---------------------------------------------------------------------------*/
PROCESS(mqtt_mote_process, "MQTT mote");
/*---------------------------------------------------------------------------*/
int
ipaddr_sprintf(char *buf, uint8_t buf_len, const uip_ipaddr_t *addr)
{
    uint16_t a;
    uint8_t len = 0;
    int i, f;
    for(i = 0, f = 0; i < sizeof(uip_ipaddr_t); i += 2) {
        a = (addr->u8[i] << 8) + addr->u8[i + 1];
        if(a == 0 && f >= 0) {
            if(f++ == 0) {
                len += snprintf(&buf[len], buf_len - len, "::");
            }
        } else {
            if(f > 0) {
                f = -1;
            } else if(i > 0) {
                len += snprintf(&buf[len], buf_len - len, ":");
            }
            len += snprintf(&buf[len], buf_len - len, "%x", a);
        }
    }

    return len;
}
/*---------------------------------------------------------------------------*/
static void
publish_led_off(void *d)
{
    leds_off(STATUS_LED);
}
/*---------------------------------------------------------------------------*/
static void
pub_handler(const char *topic, uint16_t topic_len, const uint8_t *chunk,
            uint16_t chunk_len)
{
    LOG_INFO("Pub handler: topic='%s' (len=%u), chunk_len=%u\n", topic, topic_len, chunk_len);
    snprintf(location_topic, chunk_len + 1, "%s", chunk);
    id_not_yet_set = 0;
    /* If the format != json, ignore */
    /*if(strncmp(&topic[topic_len - 4], "json", 4) != 0) {
        LOG_ERR("Incorrect format\n");
        return;
    }*/
}
/*---------------------------------------------------------------------------*/
static void
mqtt_event(struct mqtt_connection *m, mqtt_event_t event, void *data)
{
    switch(event) {
        case MQTT_EVENT_CONNECTED: {
            LOG_INFO("Application has a MQTT connection!\n");
            timer_set(&connection_life, CONNECTION_STABLE_TIME);
            state = STATE_CONNECTED;
            break;
        }
        case MQTT_EVENT_DISCONNECTED: {
            LOG_INFO("MQTT Disconnect: reason %u\n", *((mqtt_event_t *)data));

            state = STATE_DISCONNECTED;
            process_poll(&mqtt_mote_process);
            break;
        }
        case MQTT_EVENT_PUBLISH: {
            msg_ptr = data;

            if(msg_ptr->first_chunk) {
                msg_ptr->first_chunk = 0;
                LOG_INFO("Application received a publish on topic '%s'; payload "
                         "size is %i bytes\n",
                         msg_ptr->topic, msg_ptr->payload_length);
            }

            pub_handler(msg_ptr->topic, strlen(msg_ptr->topic), msg_ptr->payload_chunk,
                        msg_ptr->payload_length);
            break;
        }
        case MQTT_EVENT_SUBACK: {
            LOG_INFO("Application is subscribed to topic successfully\n");
            break;
        }
        case MQTT_EVENT_UNSUBACK: {
            LOG_INFO("Application is unsubscribed to topic successfully\n");
            break;
        }
        case MQTT_EVENT_PUBACK: {
            LOG_INFO("Publishing complete\n");
            break;
        }
        default:
            LOG_WARN("Application got a unhandled MQTT event: %i\n", event);
            break;
    }
}
/*---------------------------------------------------------------------------*/
static int
construct_pub_topic_conf(void)
{
    int len = snprintf(pub_topic, BUFFER_SIZE, PUBLISH_CONF_TOPIC);

    /* len < 0: Error. Len >= BUFFER_SIZE: Buffer too small */
    if(len < 0 || len >= BUFFER_SIZE) {
        LOG_ERR("Pub topic: %d, buffer %d\n", len, BUFFER_SIZE);
        return 0;
    }

    return 1;
}
/*---------------------------------------------------------------------------*/
static int
construct_sub_topic_conf(void)
{

    int len = snprintf(sub_topic, BUFFER_SIZE, SUB_CONF_TOPIC"%s", client_py_id);

    /* len < 0: Error. Len >= BUFFER_SIZE: Buffer too small */
    if(len < 0 || len >= BUFFER_SIZE) {
        LOG_INFO("Sub topic: %d, buffer %d\n", len, BUFFER_SIZE);
        return 0;
    }

    return 1;
}
/*---------------------------------------------------------------------------*/
static int
construct_pub_topic(void)
{
    int len = snprintf(pub_topic, BUFFER_SIZE, PUBLISH_LOCATION"%s", location_topic);

    if(len < 0 || len >= BUFFER_SIZE) {
        LOG_ERR("Pub topic: %d, buffer %d\n", len, BUFFER_SIZE);
        return 0;
    }

    return 1;
}
/*---------------------------------------------------------------------------*/
static void
set_client_id(void)
{
    snprintf(client_py_id, BUFFER_SIZE, "%s-%02x%02x",
             conf.org_id, linkaddr_node_addr.u8[6], linkaddr_node_addr.u8[7]);
}
/*---------------------------------------------------------------------------*/
static int
construct_client_id(void)
{
    int len = snprintf(client_id, BUFFER_SIZE, "d:%s:%s:%02x%02x%02x%02x%02x%02x",
                       conf.org_id, conf.type_id,
                       linkaddr_node_addr.u8[0], linkaddr_node_addr.u8[1],
                       linkaddr_node_addr.u8[2], linkaddr_node_addr.u8[5],
                       linkaddr_node_addr.u8[6], linkaddr_node_addr.u8[7]);

    /* len < 0: Error. Len >= BUFFER_SIZE: Buffer too small */
    if(len < 0 || len >= BUFFER_SIZE) {
        LOG_INFO("Client ID: %d, Buffer %d\n", len, BUFFER_SIZE);
        return 0;
    }

    set_client_id();

    return 1;
}
/*---------------------------------------------------------------------------*/
static void
update_config(int id_not_yet_set)
{
    if (id_not_yet_set) {
        if(construct_client_id() == 0) {
            /* Fatal error. Client ID larger than the buffer */
            state = STATE_CONFIG_ERROR;
            return;
        }

        if(construct_sub_topic_conf() == 0) {
            /* Fatal error. Topic larger than the buffer */
            state = STATE_CONFIG_ERROR;
            return;
        }

        if(construct_pub_topic_conf() == 0) {
            /* Fatal error. Topic larger than the buffer */
            state = STATE_CONFIG_ERROR;
            return;
        }
    } else {
        if(construct_pub_topic() == 0) {
            state = STATE_CONFIG_ERROR;
            return;
        }
    }

    /* Reset the counter */
    seq_nr_value = 0;

    state = STATE_INIT;

    /*
     * Schedule next timer event ASAP
     *
     * If we entered an error state then we won't do anything when it fires
     *
     * Since the error at this stage is a config error, we will only exit this
     * error state if we get a new config
     */
    etimer_set(&publish_periodic_timer, 0);

    return;
}
/*---------------------------------------------------------------------------*/
static void
init_config()
{
    /* Populate configuration with default values */
    memset(&conf, 0, sizeof(mqtt_client_config_t));

    memcpy(conf.org_id, DEFAULT_ORG_ID, strlen(DEFAULT_ORG_ID));
    memcpy(conf.type_id, DEFAULT_TYPE_ID, strlen(DEFAULT_TYPE_ID));
    memcpy(conf.auth_token, DEFAULT_AUTH_TOKEN, strlen(DEFAULT_AUTH_TOKEN));
    memcpy(conf.broker_ip, broker_ip, strlen(broker_ip));
    memcpy(conf.cmd_type, DEFAULT_SUBSCRIBE_CMD_TYPE, 1);

    conf.broker_port = DEFAULT_BROKER_PORT;
    conf.pub_interval = DEFAULT_PUBLISH_INTERVAL;
}
/*---------------------------------------------------------------------------*/
static void
subscribe(void)
{
    mqtt_status_t status;

    status = mqtt_subscribe(&conn, NULL, sub_topic, MQTT_QOS_LEVEL_0);

    LOG_INFO("Subscribing\n");
    if(status == MQTT_STATUS_OUT_QUEUE_FULL) {
        LOG_INFO("Tried to subscribe but command queue was full!\n");
    }
}
/*---------------------------------------------------------------------------*/
static float
get_onboard_temp(void)
{
    float min = 15.00;
    float max = 35.00;
    //return a value between 15 and 35
    return (min + 1) + (((float) rand()) / (float) RAND_MAX) * (max - (min + 1));
}
/*---------------------------------------------------------------------------*/
static float
get_onboard_hum(void)
{
    float min = 55.00;
    float max = 75.00;
    //return a value between 55 and 75
    return (min + 1) + (((float) rand()) / (float) RAND_MAX) * (max - (min + 1));
}
/*---------------------------------------------------------------------------*/
static void
publish(void)
{
    int len;
    int remaining = APP_BUFFER_SIZE;

    snprintf(value_temp, 6, "%.2f", get_onboard_temp());
    value_temp[2] = '.';
    snprintf(value_hum, 6, "%.2f", get_onboard_hum());
    value_hum[2] = '.';

    seq_nr_value++;

    buf_ptr = app_buffer;

    len = snprintf(buf_ptr, remaining,
                   "{"
                   "\"d\":{"
                   "\"s_id\":\"%s\","
		   "\"seq\":%d,"
                   "\"temp_c\":%s,"
                   "\"hum\":%s",
                   client_py_id, seq_nr_value, value_temp, value_hum);

    if(len < 0 || len >= remaining) {
        LOG_ERR("Buffer too short. Have %d, need %d + \\0\n", remaining, len);
        return;
    }

    remaining -= len;
    buf_ptr += len;

    char def_rt_str[64];
    memset(def_rt_str, 0, sizeof(def_rt_str));
    ipaddr_sprintf(def_rt_str, sizeof(def_rt_str), uip_ds6_defrt_choose());

    len = snprintf(buf_ptr, remaining, ",\"Def Route\":\"%s\"",
                   def_rt_str);

    if(len < 0 || len >= remaining) {
        LOG_ERR("Buffer too short. Have %d, need %d + \\0\n", remaining, len);
        return;
    }
    remaining -= len;
    buf_ptr += len;

    len = snprintf(buf_ptr, remaining, "}}");

    if(len < 0 || len >= remaining) {
        LOG_ERR("Buffer too short. Have %d, need %d + \\0\n", remaining, len);
        return;
    }

    mqtt_publish(&conn, NULL, pub_topic, (uint8_t *)app_buffer,
                 strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);

    LOG_INFO("Publish sent out!\n");
}
/*---------------------------------------------------------------------------*/
static void
publish_conf(void)
{
    /* Publish MQTT topic */
    int len;
    int remaining = APP_BUFFER_SIZE;

    seq_nr_value++;

    buf_ptr = app_buffer;

    len = snprintf(buf_ptr, remaining, "%s", client_py_id);

    if(len < 0 || len >= remaining) {
        LOG_ERR("Buffer too short. Have %d, need %d + \\0\n", remaining, len);
        return;
    }

    mqtt_publish(&conn, NULL, pub_topic, (uint8_t *)app_buffer,
                 strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);

    LOG_INFO("Publish sent out!\n");
}
/*---------------------------------------------------------------------------*/
static void
connect_to_broker(void)
{
    /* Connect to MQTT server */
    mqtt_connect(&conn, conf.broker_ip, conf.broker_port,
                 conf.pub_interval * 3);

    state = STATE_CONNECTING;
}
/*---------------------------------------------------------------------------*/
static void
state_machine(void)
{
    switch(state) {
        case STATE_INIT:
            mqtt_register(&conn, &mqtt_mote_process, client_id, mqtt_event,
                          MAX_TCP_SEGMENT_SIZE);

            mqtt_set_username_password(&conn, "use-token-auth",
                                       conf.auth_token);

            conn.auto_reconnect = 0;
            connect_attempt = 1;

            state = STATE_REGISTERED;
            LOG_INFO("Init\n");
        case STATE_REGISTERED:
            if(uip_ds6_get_global(ADDR_PREFERRED) != NULL) {
                LOG_INFO("Joined network! Connect attempt %u\n", connect_attempt);
                connect_to_broker();
            } else {
                leds_on(STATUS_LED);
                ctimer_set(&ct, NO_NET_LED_DURATION, publish_led_off, NULL);
            }
            etimer_set(&publish_periodic_timer, NET_CONNECT_PERIODIC);
            return;
            break;
        case STATE_CONNECTING:
            leds_on(STATUS_LED);
            ctimer_set(&ct, CONNECTING_LED_DURATION, publish_led_off, NULL);
            LOG_INFO("Connecting: retry %u...\n", connect_attempt);
            break;
	case STATE_CONNECTED:
	    if (!id_not_yet_set) {
		state = STATE_PUBLISHING;
		break;
	    }
	case STATE_PUBLISHING_CONF:
            if(timer_expired(&connection_life)) {
                connect_attempt = 0;
            }

            if(mqtt_ready(&conn) && conn.out_buffer_sent) {
                if(state == STATE_CONNECTED) {
                    subscribe();
                    state = STATE_PUBLISHING_CONF;
                } else {
                    leds_on(STATUS_LED);
                    ctimer_set(&ct, PUBLISH_LED_ON_DURATION, publish_led_off, NULL);
                    publish_conf();
		    state = STATE_LISTENING;
                }
                etimer_set(&publish_periodic_timer, conf.pub_interval);

                LOG_INFO("Publishing\n");
                return;
            } else {
                LOG_INFO("Publishing... (MQTT state=%d, q=%u)\n", conn.state, conn.out_queue_full);
            }
            break;
	case STATE_LISTENING:
	    if (!id_not_yet_set){
		mqtt_status_t status;
		status = mqtt_unsubscribe(&conn, NULL, sub_topic);
		if (status == MQTT_STATUS_NOT_CONNECTED_ERROR) {
        		LOG_INFO("Not connected to the broker!\n");
		} else {
			LOG_INFO("Unsubscribing\n");
    			if(status == MQTT_STATUS_OUT_QUEUE_FULL) {
        			LOG_INFO("Tried to unsubscribe but command queue was full!\n");
    			}
		}
		
		update_config(id_not_yet_set);
		state = STATE_PUBLISHING;
	    } 
	    break;
        case STATE_PUBLISHING:
            if(timer_expired(&connection_life)) {
                connect_attempt = 0;
            }

            if(mqtt_ready(&conn) && conn.out_buffer_sent) {
                leds_on(STATUS_LED);
                ctimer_set(&ct, PUBLISH_LED_ON_DURATION, publish_led_off, NULL);
                publish();

                etimer_set(&publish_periodic_timer, conf.pub_interval);

                LOG_INFO("Publishing\n");
                return;
            } else {
                LOG_INFO("Publishing... (MQTT state=%d, q=%u)\n", conn.state, conn.out_queue_full);
            }
            break;
        case STATE_DISCONNECTED:
            LOG_INFO("Disconnected\n");
            if(connect_attempt < RECONNECT_ATTEMPTS ||
               RECONNECT_ATTEMPTS == RETRY_FOREVER) {
                /* Disconnect and backoff */
                clock_time_t interval;
                mqtt_disconnect(&conn);
                connect_attempt++;

                interval = connect_attempt < 3 ? RECONNECT_INTERVAL << connect_attempt :
                           RECONNECT_INTERVAL << 3;

                LOG_INFO("Disconnected: attempt %u in %lu ticks\n", connect_attempt, interval);

                etimer_set(&publish_periodic_timer, interval);

                state = STATE_REGISTERED;
                return;
            } else {
                /* Max reconnect attempts reached; enter error state */
                state = STATE_ERROR;
                LOG_ERR("Aborting connection after %u attempts\n", connect_attempt - 1);
            }
            break;
        case STATE_CONFIG_ERROR:
            /* Idle away. The only way out is a new config */
            LOG_ERR("Bad configuration.\n");
            return;
        case STATE_ERROR:
        default:
            leds_on(STATUS_LED);
            LOG_INFO("Default case: State=0x%02x\n", state);
            return;
    }

    /* If we didn't return so far, reschedule ourselves */
    etimer_set(&publish_periodic_timer, STATE_MACHINE_PERIODIC);
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(mqtt_mote_process, ev, data)
{

    PROCESS_BEGIN();

    LOG_INFO("MQTT mote Process\n");

    init_config();
    update_config(id_not_yet_set);
    
    /* Main loop */
    while(1) {

        PROCESS_YIELD();

        if (ev == PROCESS_EVENT_TIMER && data == &publish_periodic_timer) {
            state_machine();
        }

    }

    PROCESS_END();
}
/*---------------------------------------------------------------------------*/
/**
 * @}
 * @}
 */
