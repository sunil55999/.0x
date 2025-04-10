import asyncio
import logging
import json
from telethon import TelegramClient, events, errors
from telethon.tl.types import (
    MessageMediaWebPage, MessageEntityTextUrl, MessageEntityUrl, 
    MessageMediaPhoto, MessageMediaDocument, MessageMediaPoll, 
    MessageMediaGeo, MessageMediaContact, MessageMediaVenue, 
    MessageMediaGame, MessageMediaInvoice, MessageMediaGeoLive,
    MessageMediaDice, MessageMediaStory, InputMediaPoll, Poll, 
    PollAnswer, InputReplyToMessage, Updates, UpdateNewMessage
)
from telethon.tl.functions.messages import SendMediaRequest
from collections import deque
from datetime import datetime
import imagehash
from PIL import Image
import io
import traceback
import re

# Configuration
API_ID = 23617139  # Replace with your API ID
API_HASH = "5bfc582b080fa09a1a2eaa6ee60fd5d4"  # Replace with your API hash
SESSION_FILE = "userbot_session"
client = TelegramClient(SESSION_FILE, API_ID, API_HASH)

MAPPINGS_FILE = "channel_mappings.json"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
MAX_QUEUE_SIZE = 100
MAX_MAPPING_HISTORY = 1000
MONITOR_CHAT_ID = None
NOTIFY_CHAT_ID = None
INACTIVITY_THRESHOLD = 21600  # 6 hours in seconds
MAX_MESSAGE_LENGTH = 4096  # Telegram's max message length
FORWARD_DELAY = 1  # seconds delay between forwarding messages
QUEUE_INACTIVITY_THRESHOLD = 600  # 10 minutes in seconds for queue inactivity alert

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("forward_bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger("ForwardBot")

# Data structures
channel_mappings = {}
message_queue = deque(maxlen=MAX_QUEUE_SIZE)  # Now stores (event, mapping, user_id, pair_name, queued_time)
is_connected = False
pair_stats = {}

def save_mappings():
    """Save channel mappings to file."""
    try:
        with open(MAPPINGS_FILE, "w") as f:
            json.dump(channel_mappings, f)
        logger.info("Channel mappings saved to file.")
    except Exception as e:
        logger.error(f"Error saving mappings: {e}")

def load_mappings():
    """Load channel mappings from file and initialize stats."""
    global channel_mappings
    try:
        with open(MAPPINGS_FILE, "r") as f:
            channel_mappings = json.load(f)
        logger.info(f"Loaded {sum(len(v) for v in channel_mappings.values())} mappings from file.")
        for user_id, pairs in channel_mappings.items():
            if user_id not in pair_stats:
                pair_stats[user_id] = {}
            for pair_name in pairs:
                pair_stats[user_id][pair_name] = {
                    'forwarded': 0, 'edited': 0, 'deleted': 0, 'blocked': 0, 'queued': 0, 'last_activity': None
                }
    except FileNotFoundError:
        logger.info("No existing mappings file found. Starting fresh.")
    except Exception as e:
        logger.error(f"Error loading mappings: {e}")

def filter_blacklisted_words(text, blacklist):
    """Replace blacklisted words with asterisks."""
    if not text or not blacklist:
        return text
    for word in blacklist:
        text = text.replace(word, "***")
    return text

def check_blocked_sentences(text, blocked_sentences):
    """Check if text contains blocked sentences."""
    if not text or not blocked_sentences:
        return False, None
    for sentence in blocked_sentences:
        if sentence.lower() in text.lower():
            return True, sentence
    return False, None

def filter_urls(text, block_urls, blacklist_urls=None):
    """Filter or block URLs in text."""
    if not text or not block_urls:
        return text, True
    url_pattern = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[^\s]*)?'
    urls = re.findall(url_pattern, text)
    if blacklist_urls:
        for url in urls:
            if any(blacklisted in url for blacklisted in blacklist_urls):
                text = text.replace(url, '[URL BLOCKED]')
        return text, True
    else:
        text = re.sub(url_pattern, '[URL REMOVED]', text)
        return text, False

def remove_header_footer(text, header_pattern, footer_pattern):
    """Remove specified header and footer from text."""
    if not text:
        return text
    if header_pattern and text.startswith(header_pattern):
        text = text[len(header_pattern):].strip()
    if footer_pattern and text.endswith(footer_pattern):
        text = text[:-len(footer_pattern)].strip()
    return text

def apply_custom_header_footer(text, custom_header, custom_footer):
    """Add custom header and footer to text."""
    if not text:
        return text
    result = text
    if custom_header:
        result = f"{custom_header}\n{result}"
    if custom_footer:
        result = f"{result.rstrip()}\n{custom_footer}"
    return result.strip()

async def send_split_message(client, entity, message_text, reply_to=None, silent=False, entities=None):
    """Send long messages in parts if they exceed Telegram's limit."""
    if len(message_text) <= MAX_MESSAGE_LENGTH:
        return await client.send_message(
            entity=entity,
            message=message_text,
            reply_to=reply_to,
            silent=silent,
            formatting_entities=entities if entities else None
        )
    parts = [message_text[i:i + MAX_MESSAGE_LENGTH] for i in range(0, len(message_text), MAX_MESSAGE_LENGTH)]
    sent_messages = []
    for part in parts:
        sent_msg = await client.send_message(
            entity=entity,
            message=part,
            reply_to=reply_to if not sent_messages else None,
            silent=silent,
            formatting_entities=entities if entities and not sent_messages else None
        )
        sent_messages.append(sent_msg)
        await asyncio.sleep(0.5)
    return sent_messages[0] if sent_messages else None

def extract_message_from_updates(updates):
    """Extract message from Telegram Updates object."""
    if isinstance(updates, Updates):
        for update in updates.updates:
            if isinstance(update, UpdateNewMessage):
                return update.message
        logger.error("No UpdateNewMessage found in Updates object")
        return None
    return updates

async def forward_message_with_retry(event, mapping, user_id, pair_name):
    """Forward a message with retries and filtering."""
    source_msg_id = event.message.id if hasattr(event.message, 'id') else "Unknown"
    for attempt in range(MAX_RETRIES):
        try:
            message_text = event.message.raw_text or ""
            original_entities = event.message.entities or []
            media = event.message.media
            reply_to = await handle_reply_mapping(event, mapping)

            if message_text:
                if mapping.get('blocked_sentences'):
                    should_block, matching_sentence = check_blocked_sentences(message_text, mapping['blocked_sentences'])
                    if should_block:
                        logger.info(f"Message blocked due to blocked sentence: '{matching_sentence}'")
                        pair_stats[user_id][pair_name]['blocked'] += 1
                        return True

                if mapping.get('blacklist'):
                    message_text = filter_blacklisted_words(message_text, mapping['blacklist'])
                    if message_text.strip() == "***":
                        logger.info("Message entirely blocked due to blacklist filter")
                        pair_stats[user_id][pair_name]['blocked'] += 1
                        return True

                if mapping.get('block_urls', False) or mapping.get('blacklist_urls'):
                    message_text, allow_preview = filter_urls(
                        message_text,
                        mapping.get('block_urls', False),
                        mapping.get('blacklist_urls')
                    )
                    if message_text != event.message.raw_text:
                        original_entities = None

                if mapping.get('header_pattern') or mapping.get('footer_pattern'):
                    message_text = remove_header_footer(
                        message_text, mapping.get('header_pattern', ''), mapping.get('footer_pattern', '')
                    )
                    if message_text != event.message.raw_text:
                        original_entities = None

                if mapping.get('remove_mentions', False):
                    message_text = re.sub(r'@[a-zA-Z0-9_]+|\[([^\]]+)\]\(tg://user\?id=\d+\)', '', message_text)
                    message_text = re.sub(r'\s+', ' ', message_text).strip()
                    if message_text != event.message.raw_text:
                        original_entities = None

                message_text = apply_custom_header_footer(
                    message_text, mapping.get('custom_header', ''), mapping.get('custom_footer', '')
                )
                if message_text != event.message.raw_text:
                    original_entities = None

            if media:
                if isinstance(media, MessageMediaPhoto):
                    if mapping.get('blocked_image_hashes'):
                        photo = await client.download_media(event.message, bytes)
                        image = Image.open(io.BytesIO(photo))
                        image_hash = str(imagehash.phash(image))
                        if image_hash in mapping['blocked_image_hashes']:
                            logger.info(f"Image blocked due to matching perceptual hash: {image_hash}")
                            pair_stats[user_id][pair_name]['blocked'] += 1
                            return True
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        file=media,
                        message=message_text,
                        reply_to=reply_to,
                        silent=event.message.silent,
                        formatting_entities=original_entities if original_entities else None
                    )
                elif isinstance(media, MessageMediaDocument):
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        file=media,
                        message=message_text,
                        reply_to=reply_to,
                        silent=event.message.silent,
                        formatting_entities=original_entities if original_entities else None
                    )
                elif isinstance(media, MessageMediaPoll):
                    poll = media.poll
                    options = [PollAnswer(option.text, bytes([i])) for i, option in enumerate(poll.answers)]
                    input_media_poll = InputMediaPoll(
                        poll=Poll(
                            id=poll.id,
                            question=poll.question,
                            answers=options,
                            closed=poll.closed,
                            public_voters=poll.public_voters,
                            multiple_choice=poll.multiple_choice,
                            quiz=poll.quiz
                        )
                    )
                    reply_to_obj = InputReplyToMessage(reply_to_msg_id=reply_to) if reply_to else None
                    sent_message = await client(SendMediaRequest(
                        peer=int(mapping['destination']),
                        media=input_media_poll,
                        message=message_text,
                        reply_to=reply_to_obj,
                        silent=event.message.silent,
                        entities=original_entities if original_entities else None
                    ))
                    sent_message = extract_message_from_updates(sent_message)
                    if not sent_message:
                        logger.error("Failed to extract message from poll Updates")
                        return False
                elif isinstance(media, MessageMediaGeo):
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        message=message_text or "Location",
                        geo=media,
                        reply_to=reply_to,
                        silent=event.message.silent
                    )
                elif isinstance(media, MessageMediaContact):
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        message=message_text or "Contact",
                        contact=media,
                        reply_to=reply_to,
                        silent=event.message.silent
                    )
                elif isinstance(media, MessageMediaVenue):
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        message=message_text or f"Venue: {media.title}",
                        geo=media,
                        reply_to=reply_to,
                        silent=event.message.silent
                    )
                elif isinstance(media, MessageMediaWebPage):
                    has_links = any(isinstance(e, (MessageEntityTextUrl, MessageEntityUrl)) for e in original_entities)
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        message=message_text,
                        link_preview=True if has_links else False,
                        reply_to=reply_to,
                        silent=event.message.silent,
                        formatting_entities=original_entities if original_entities else None
                    )
                elif isinstance(media, MessageMediaDice):
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        message=message_text or f"Dice: {media.emoticon}",
                        dice=media,
                        reply_to=reply_to,
                        silent=event.message.silent
                    )
                elif isinstance(media, MessageMediaGame):
                    logger.info("Games cannot be forwarded directly; sending text only")
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        message=message_text or f"Game: {media.game.title}",
                        reply_to=reply_to,
                        silent=event.message.silent
                    )
                elif isinstance(media, MessageMediaInvoice):
                    logger.info("Invoices cannot be forwarded; sending text only")
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        message=message_text or f"Invoice: {media.title}",
                        reply_to=reply_to,
                        silent=event.message.silent
                    )
                elif isinstance(media, MessageMediaGeoLive):
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        message=message_text or "Live Location",
                        geo=media,
                        reply_to=reply_to,
                        silent=event.message.silent
                    )
                elif isinstance(media, MessageMediaStory):
                    logger.info("Stories cannot be forwarded directly; sending text only")
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        message=message_text or "Story",
                        reply_to=reply_to,
                        silent=event.message.silent
                    )
                else:
                    logger.warning(f"Unsupported media type: {type(media).__name__}")
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        message=message_text or "Unsupported media type",
                        reply_to=reply_to,
                        silent=event.message.silent
                    )
            else:
                if not message_text.strip():
                    logger.info("Message skipped: empty text with no media")
                    pair_stats[user_id][pair_name]['blocked'] += 1
                    return True
                if len(message_text) > MAX_MESSAGE_LENGTH:
                    sent_message = await send_split_message(
                        client,
                        int(mapping['destination']),
                        message_text,
                        reply_to=reply_to,
                        silent=event.message.silent,
                        entities=original_entities
                    )
                else:
                    sent_message = await client.send_message(
                        entity=int(mapping['destination']),
                        message=message_text,
                        reply_to=reply_to,
                        silent=event.message.silent,
                        formatting_entities=original_entities if original_entities else None
                    )

            await store_message_mapping(event, mapping, sent_message)
            pair_stats[user_id][pair_name]['forwarded'] += 1
            pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
            logger.info(f"Message forwarded from {mapping['source']} to {mapping['destination']} (ID: {sent_message.id})")
            return True

        except errors.FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"Flood wait error, sleeping for {wait_time} seconds for pair '{pair_name}' (Source Msg ID: {source_msg_id})")
            await asyncio.sleep(wait_time)
        except errors.MessageTooLongError:
            logger.warning(f"Message too long; splitting and retrying for pair '{pair_name}' (Source Msg ID: {source_msg_id})")
            sent_message = await send_split_message(
                client,
                int(mapping['destination']),
                message_text,
                reply_to=reply_to,
                silent=event.message.silent,
                entities=original_entities
            )
            if sent_message:
                await store_message_mapping(event, mapping, sent_message)
                pair_stats[user_id][pair_name]['forwarded'] += 1
                pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
                logger.info(f"Long message forwarded from {mapping['source']} to {mapping['destination']} (ID: {sent_message.id})")
                return True
            return False
        except (errors.RPCError, ConnectionError) as e:
            logger.warning(f"Attempt {attempt + 1} failed for pair '{pair_name}' (Source Msg ID: {source_msg_id}): {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
            else:
                error_msg = f"❌ Failed to forward message for pair '{pair_name}' (Source Msg ID: {source_msg_id}) after {MAX_RETRIES} attempts. Error: {e}"
                logger.error(error_msg)
                if NOTIFY_CHAT_ID:
                    await client.send_message(NOTIFY_CHAT_ID, error_msg)
                return False
        except Exception as e:
            error_msg = f"⚠️ Unexpected error forwarding message for pair '{pair_name}' (Source Msg ID: {source_msg_id}): {e}"
            logger.error(error_msg, exc_info=True)
            if NOTIFY_CHAT_ID:
                await client.send_message(NOTIFY_CHAT_ID, error_msg)
            return False

async def edit_forwarded_message(event, mapping, user_id, pair_name):
    """Edit a forwarded message based on source changes."""
    try:
        mapping_key = f"{mapping['source']}:{event.message.id}"
        if not hasattr(client, 'forwarded_messages'):
            logger.warning("No forwarded_messages attribute found on client")
            return
        if mapping_key not in client.forwarded_messages:
            logger.warning(f"No mapping found for message: {mapping_key}")
            return

        forwarded_msg_id = client.forwarded_messages[mapping_key]
        forwarded_msg = await client.get_messages(int(mapping['destination']), ids=forwarded_msg_id)
        if not forwarded_msg:
            logger.warning(f"Forwarded message {forwarded_msg_id} not found in destination {mapping['destination']}")
            del client.forwarded_messages[mapping_key]
            return

        message_text = event.message.raw_text or ""
        original_entities = event.message.entities or []
        media = event.message.media

        if isinstance(media, MessageMediaPhoto) and mapping.get('blocked_image_hashes'):
            photo = await client.download_media(event.message, bytes)
            image = Image.open(io.BytesIO(photo))
            image_hash = str(imagehash.phash(image))
            if image_hash in mapping['blocked_image_hashes']:
                await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
                logger.info(f"Forwarded message {forwarded_msg_id} deleted due to blocked image hash: {image_hash}")
                pair_stats[user_id][pair_name]['blocked'] += 1
                pair_stats[user_id][pair_name]['deleted'] += 1
                return

        if mapping.get('blocked_sentences'):
            should_block, matching_sentence = check_blocked_sentences(message_text, mapping['blocked_sentences'])
            if should_block:
                await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
                logger.info(f"Forwarded message {forwarded_msg_id} deleted due to blocked sentence: '{matching_sentence}'")
                pair_stats[user_id][pair_name]['blocked'] += 1
                pair_stats[user_id][pair_name]['deleted'] += 1
                return

        if mapping.get('blacklist') and message_text:
            message_text = filter_blacklisted_words(message_text, mapping['blacklist'])
            if message_text.strip() == "***":
                await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
                logger.info(f"Forwarded message {forwarded_msg_id} deleted due to blacklist filter")
                pair_stats[user_id][pair_name]['blocked'] += 1
                pair_stats[user_id][pair_name]['deleted'] += 1
                return

        if mapping.get('block_urls', False) or mapping.get('blacklist_urls'):
            message_text, _ = filter_urls(
                message_text, 
                mapping.get('block_urls', False), 
                mapping.get('blacklist_urls')
            )
            if message_text != event.message.raw_text:
                original_entities = None

        if (mapping.get('header_pattern') or mapping.get('footer_pattern')) and message_text:
            message_text = remove_header_footer(
                message_text, mapping.get('header_pattern', ''), mapping.get('footer_pattern', '')
            )
            if message_text != event.message.raw_text:
                original_entities = None

        if mapping.get('remove_mentions', False) and message_text:
            message_text = re.sub(r'@[a-zA-Z0-9_]+|\[([^\]]+)\]\(tg://user\?id=\d+\)', '', message_text)
            message_text = re.sub(r'\s+', ' ', message_text).strip()
            if message_text != event.message.raw_text:
                original_entities = None

        if not message_text.strip() and not media:
            await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
            logger.info(f"Forwarded message {forwarded_msg_id} deleted: empty after filtering")
            pair_stats[user_id][pair_name]['blocked'] += 1
            pair_stats[user_id][pair_name]['deleted'] += 1
            return

        message_text = apply_custom_header_footer(
            message_text, mapping.get('custom_header', ''), mapping.get('custom_footer', '')
        )
        if message_text != event.message.raw_text:
            original_entities = None

        if isinstance(media, MessageMediaPoll):
            logger.info(f"Poll message {forwarded_msg_id} cannot be edited; deleting and resending")
            await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
            del client.forwarded_messages[mapping_key]
            await forward_message_with_retry(event, mapping, user_id, pair_name)
            return

        await client.edit_message(
            entity=int(mapping['destination']),
            message=forwarded_msg_id,
            text=message_text,
            file=media if media and isinstance(media, (MessageMediaPhoto, MessageMediaDocument)) else None,
            formatting_entities=original_entities if original_entities else None
        )
        pair_stats[user_id][pair_name]['edited'] += 1
        pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
        logger.info(f"Forwarded message {forwarded_msg_id} edited in {mapping['destination']}")

    except errors.MessageAuthorRequiredError:
        logger.error(f"Cannot edit message {forwarded_msg_id}: Bot must be the original author")
    except errors.MessageIdInvalidError:
        logger.error(f"Cannot edit message {forwarded_msg_id}: Message ID is invalid or deleted")
        if mapping_key in client.forwarded_messages:
            del client.forwarded_messages[mapping_key]
    except errors.FloodWaitError as e:
        logger.warning(f"Flood wait error while editing, sleeping for {e.seconds} seconds...")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logger.error(f"Error editing forwarded message {forwarded_msg_id}: {e}")

async def delete_forwarded_message(event, mapping, user_id, pair_name):
    """Delete a forwarded message if the source message is deleted."""
    try:
        mapping_key = f"{mapping['source']}:{event.message.id}"
        if not hasattr(client, 'forwarded_messages'):
            logger.warning("No forwarded_messages attribute found on client")
            return
        if mapping_key not in client.forwarded_messages:
            logger.warning(f"No mapping found for deleted message: {mapping_key}")
            return

        forwarded_msg_id = client.forwarded_messages[mapping_key]
        await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
        pair_stats[user_id][pair_name]['deleted'] += 1
        pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
        logger.info(f"Forwarded message {forwarded_msg_id} deleted from {mapping['destination']}")
        del client.forwarded_messages[mapping_key]

    except errors.MessageIdInvalidError:
        logger.warning(f"Cannot delete message {forwarded_msg_id}: Already deleted or invalid")
        if mapping_key in client.forwarded_messages:
            del client.forwarded_messages[mapping_key]
    except Exception as e:
        logger.error(f"Error deleting forwarded message: {e}")

async def handle_reply_mapping(event, mapping):
    """Map reply-to messages from source to destination."""
    if not hasattr(event.message, 'reply_to') or not event.message.reply_to:
        return None
    try:
        source_reply_id = event.message.reply_to.reply_to_msg_id
        if not source_reply_id:
            return None
        mapping_key = f"{mapping['source']}:{source_reply_id}"
        if hasattr(client, 'forwarded_messages') and mapping_key in client.forwarded_messages:
            return client.forwarded_messages[mapping_key]
        replied_msg = await client.get_messages(int(mapping['source']), ids=source_reply_id)
        if replied_msg and replied_msg.text:
            dest_msgs = await client.get_messages(int(mapping['destination']), search=replied_msg.text[:20], limit=5)
            if dest_msgs:
                return dest_msgs[0].id
    except Exception as e:
        logger.error(f"Error handling reply mapping: {e}")
    return None

async def store_message_mapping(event, mapping, sent_message):
    """Store mapping of source to forwarded message IDs."""
    try:
        if not hasattr(event.message, 'id'):
            return
        if not hasattr(client, 'forwarded_messages'):
            client.forwarded_messages = {}
        if len(client.forwarded_messages) >= MAX_MAPPING_HISTORY:
            oldest_key = next(iter(client.forwarded_messages))
            client.forwarded_messages.pop(oldest_key)
        source_msg_id = event.message.id
        mapping_key = f"{mapping['source']}:{source_msg_id}"
        client.forwarded_messages[mapping_key] = sent_message.id
    except Exception as e:
        logger.error(f"Error storing message mapping: {e}")

async def send_split_message_event(event, full_message):
    """Send long messages in parts for event replies."""
    if len(full_message) <= MAX_MESSAGE_LENGTH:
        await event.reply(full_message)
        return

    parts = []
    current_part = ""
    for line in full_message.split('\n'):
        if len(current_part) + len(line) + 1 > MAX_MESSAGE_LENGTH:
            parts.append(current_part.strip())
            current_part = line + "\n"
        else:
            current_part += line + "\n"
    if current_part.strip():
        parts.append(current_part.strip())

    for i, part in enumerate(parts, 1):
        await event.reply(f"📜 Part {i}/{len(parts)}\n{part}")
        await asyncio.sleep(0.5)

@client.on(events.NewMessage(pattern='(?i)^/start$'))
async def start(event):
    await event.reply("✅ ForwardBot Running!\nUse `/commands` for options.")

@client.on(events.NewMessage(pattern='(?i)^/commands$'))
async def list_commands(event):
    commands = """
    📋 ForwardBot Commands

    **Setup & Management**
    - `/setpair <name> <source> <dest> [yes|no]` - Add a forwarding pair (yes/no for mentions)
    - `/listpairs` - Show all pairs
    - `/pausepair <name>` - Pause a pair
    - `/startpair <name>` - Resume a pair
    - `/clearpairs` - Remove all pairs
    - `/togglementions <name>` - Toggle mention removal
    - `/monitor` - View pair stats
    - `/status` - Check bot status

    **🔍 Filters**
    - `/addblacklist <name> <word1,word2,...>` - Blacklist words
    - `/clearblacklist <name>` - Clear blacklist
    - `/showblacklist <name>` - Show blacklist
    - `/toggleurlblock <name>` - Toggle URL blocking
    - `/addurlblacklist <name> <url1,url2,...>` - Blacklist specific URLs
    - `/clearurlblacklist <name>` - Clear URL blacklist
    - `/setheader <name> <text>` - Set header to remove
    - `/setfooter <name> <text>` - Set footer to remove
    - `/clearheaderfooter <name>` - Clear header/footer

    **🖼️ Image Blocking**
    - `/blockimage <name>` - Block a specific image (reply to image)
    - `/clearblockedimages <name>` - Clear blocked images
    - `/showblockedimages <name>` - Show blocked image hashes

    **✍️ Custom Text**
    - `/setcustomheader <name> <text>` - Set custom header
    - `/setcustomfooter <name> <text>` - Set custom footer
    - `/clearcustomheaderfooter <name>` - Clear custom text

    **🚫 Blocking**
    - `/blocksentence <name> <sentence>` - Block a sentence
    - `/clearblocksentences <name>` - Clear blocked sentences
    - `/showblocksentences <name>` - Show blocked sentences
    """
    await event.reply(commands)

@client.on(events.NewMessage(pattern='(?i)^/status$'))
async def status(event):
    status_msg = f"🛠️ Bot Status\n" \
                 f"📡 Connected: {'✅' if is_connected else '❌'}\n" \
                 f"📥 Queue Size: {len(message_queue)}/{MAX_QUEUE_SIZE}\n" \
                 f"📊 Total Pairs: {sum(len(pairs) for pairs in channel_mappings.values())}"
    await event.reply(status_msg)

@client.on(events.NewMessage(pattern='(?i)^/monitor$'))
async def monitor_pairs(event):
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or not channel_mappings[user_id]:
        await event.reply("❌ No forwarding pairs found.")
        return

    header = "📊 Forwarding Monitor\n--------------------\n"
    footer = f"\n--------------------\n📥 Total Queued: {len(message_queue)}"
    report = []
    for pair_name, data in channel_mappings[user_id].items():
        stats = pair_stats.get(user_id, {}).get(pair_name, {
            'forwarded': 0, 'edited': 0, 'deleted': 0, 'blocked': 0, 'queued': 0, 'last_activity': None
        })
        last_activity = stats['last_activity'] or 'N/A'
        if len(last_activity) > 20:
            last_activity = last_activity[:17] + "..."
        report.append(
            f"📌 {pair_name}\n"
            f"   ➡️ Route: {data['source']} → {data['destination']}\n"
            f"   ✅ Status: {'Active' if data['active'] else 'Paused'}\n"
            f"   📈 Stats: Fwd: {stats['forwarded']} | Edt: {stats['edited']} | Del: {stats['deleted']} | Blk: {stats['blocked']} | Que: {stats['queued']}\n"
            f"   ⏰ Last: {last_activity}\n"
            f"---------------"
        )
    full_message = header + "\n".join(report) + footer
    await send_split_message_event(event, full_message)

@client.on(events.NewMessage(pattern=r'/setpair (\S+) (\S+) (\S+)(?: (yes|no))?'))
async def set_pair(event):
    pair_name, source, destination, remove_mentions = event.pattern_match.groups()
    user_id = str(event.sender_id)
    remove_mentions = remove_mentions == "yes"

    logger.info(f"Setting pair {pair_name} for user {user_id}: {source} -> {destination}")

    if user_id not in channel_mappings:
        channel_mappings[user_id] = {}
        logger.info(f"Created new mapping dictionary for user {user_id}")
    if user_id not in pair_stats:
        pair_stats[user_id] = {}

    channel_mappings[user_id][pair_name] = {
        'source': source,
        'destination': destination,
        'active': True,
        'remove_mentions': remove_mentions,
        'blacklist': [],
        'block_urls': False,
        'blacklist_urls': [],
        'header_pattern': '',
        'footer_pattern': '',
        'custom_header': '',
        'custom_footer': '',
        'blocked_sentences': [],
        'blocked_image_hashes': []
    }
    pair_stats[user_id][pair_name] = {'forwarded': 0, 'edited': 0, 'deleted': 0, 'blocked': 0, 'queued': 0, 'last_activity': None}
    save_mappings()
    logger.info(f"Pair {pair_name} successfully set for user {user_id}")
    await event.reply(f"✅ Pair '{pair_name}' Added\n{source} ➡️ {destination}\nMentions: {'✅' if remove_mentions else '❌'}")

@client.on(events.NewMessage(pattern=r'/blockimage (\S+)'))
async def block_image(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)

    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found. Use /listpairs or /setpair.")
        return

    if not event.message.reply_to:
        await event.reply("📷 Please reply to an image to block it.")
        return

    replied_msg = await event.get_reply_message()
    if not isinstance(replied_msg.media, MessageMediaPhoto):
        await event.reply("📷 Please reply to a photo message.")
        return

    try:
        photo = await client.download_media(replied_msg, bytes)
        image = Image.open(io.BytesIO(photo))
        image_hash = str(imagehash.phash(image))

        channel_mappings[user_id][pair_name].setdefault('blocked_image_hashes', []).append(image_hash)
        channel_mappings[user_id][pair_name]['blocked_image_hashes'] = list(set(channel_mappings[user_id][pair_name]['blocked_image_hashes']))
        save_mappings()

        logger.info(f"Blocked image hash {image_hash} for pair {pair_name} by user {user_id}")
        await event.reply(f"🖼️ Image hash {image_hash} blocked for '{pair_name}'")
    except Exception as e:
        logger.error(f"Error blocking image: {e}", exc_info=True)
        await event.reply(f"❌ Error blocking image: {str(e)}")

@client.on(events.NewMessage(pattern='(?i)^/clearblockedimages (\S+)$'))
async def clear_blocked_images(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)

    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['blocked_image_hashes'] = []
        save_mappings()
        await event.reply(f"🖼️ Blocked images cleared for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/showblockedimages (\S+)$'))
async def show_blocked_images(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)

    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        blocked_hashes = channel_mappings[user_id][pair_name].get('blocked_image_hashes', [])
        if blocked_hashes:
            hashes_list = "\n".join([f"• {h}" for h in blocked_hashes])
            await event.reply(f"🖼️ Blocked Image Hashes for '{pair_name}'\n{hashes_list}")
        else:
            await event.reply(f"ℹ️ No blocked images for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern=r'/blocksentence (\S+) (.+)'))
async def block_sentence(event):
    pair_name, sentence = event.pattern_match.group(1), event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name].setdefault('blocked_sentences', []).append(sentence)
        save_mappings()
        await event.reply(f"🚫 Blocked sentence added for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearblocksentences (\S+)$'))
async def clear_block_sentences(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['blocked_sentences'] = []
        save_mappings()
        await event.reply(f"🚫 Blocked sentences cleared for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/showblocksentences (\S+)$'))
async def show_block_sentences(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        blocked_sentences = channel_mappings[user_id][pair_name].get('blocked_sentences', [])
        if blocked_sentences:
            sentences_list = "\n".join([f"• {s}" for s in blocked_sentences])
            await event.reply(f"🚫 Blocked Sentences for '{pair_name}'\n{sentences_list}")
        else:
            await event.reply(f"ℹ️ No blocked sentences for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern=r'/addblacklist (\S+) (.+)'))
async def add_blacklist(event):
    pair_name, words = event.pattern_match.group(1), event.pattern_match.group(2).split(',')
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name].setdefault('blacklist', []).extend([w.strip() for w in words])
        channel_mappings[user_id][pair_name]['blacklist'] = list(set(channel_mappings[user_id][pair_name]['blacklist']))
        save_mappings()
        await event.reply(f"🔍 Added {len(words)} word(s) to blacklist for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearblacklist (\S+)$'))
async def clear_blacklist(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['blacklist'] = []
        save_mappings()
        await event.reply(f"🔍 Blacklist cleared for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/showblacklist (\S+)$'))
async def show_blacklist(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        blacklist = channel_mappings[user_id][pair_name].get('blacklist', [])
        if blacklist:
            words_list = ", ".join(blacklist)
            await event.reply(f"🔍 Blacklist for '{pair_name}'\n{words_list}")
        else:
            await event.reply(f"ℹ️ No blacklisted words for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/toggleurlblock (\S+)$'))
async def toggle_url_block(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        current_status = channel_mappings[user_id][pair_name].get('block_urls', False)
        channel_mappings[user_id][pair_name]['block_urls'] = not current_status
        save_mappings()
        status = "ENABLED" if not current_status else "DISABLED"
        await event.reply(f"🔗 URL Blocking {status} for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern=r'/addurlblacklist (\S+) (.+)'))
async def add_url_blacklist(event):
    pair_name, urls = event.pattern_match.group(1), event.pattern_match.group(2).split(',')
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name].setdefault('blacklist_urls', []).extend([u.strip() for u in urls])
        channel_mappings[user_id][pair_name]['blacklist_urls'] = list(set(channel_mappings[user_id][pair_name]['blacklist_urls']))
        save_mappings()
        await event.reply(f"🔗 Added {len(urls)} URL(s) to blacklist for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearurlblacklist (\S+)$'))
async def clear_url_blacklist(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['blacklist_urls'] = []
        save_mappings()
        await event.reply(f"🔗 URL blacklist cleared for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern=r'/setheader (\S+) (.+)'))
async def set_header(event):
    pair_name, pattern = event.pattern_match.group(1), event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['header_pattern'] = pattern
        save_mappings()
        await event.reply(f"📑 Header set for '{pair_name}': '{pattern}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern=r'/setfooter (\S+) (.+)'))
async def set_footer(event):
    pair_name, pattern = event.pattern_match.group(1), event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['footer_pattern'] = pattern
        save_mappings()
        await event.reply(f"📑 Footer set for '{pair_name}': '{pattern}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearheaderfooter (\S+)$'))
async def clear_header_footer(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['header_pattern'] = ''
        channel_mappings[user_id][pair_name]['footer_pattern'] = ''
        save_mappings()
        await event.reply(f"📑 Header/Footer cleared for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern=r'/setcustomheader (\S+) (.+)'))
async def set_custom_header(event):
    pair_name, text = event.pattern_match.group(1), event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['custom_header'] = text
        save_mappings()
        await event.reply(f"✍️ Custom header set for '{pair_name}': '{text}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern=r'/setcustomfooter (\S+) (.+)'))
async def set_custom_footer(event):
    pair_name, text = event.pattern_match.group(1), event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['custom_footer'] = text
        save_mappings()
        await event.reply(f"✍️ Custom footer set for '{pair_name}': '{text}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearcustomheaderfooter (\S+)$'))
async def clear_custom_header_footer(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['custom_header'] = ''
        channel_mappings[user_id][pair_name]['custom_footer'] = ''
        save_mappings()
        await event.reply(f"✍️ Custom header/footer cleared for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/togglementions (\S+)$'))
async def toggle_mentions(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        current_status = channel_mappings[user_id][pair_name]['remove_mentions']
        channel_mappings[user_id][pair_name]['remove_mentions'] = not current_status
        save_mappings()
        status = "ENABLED" if not current_status else "DISABLED"
        await event.reply(f"👤 Mention removal {status} for '{pair_name}'")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/listpairs$'))
async def list_pairs(event):
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or not channel_mappings[user_id]:
        await event.reply("❌ No forwarding pairs found.")
        return

    header = "📋 Forwarding Pairs List\n--------------------\n"
    pairs_list = []
    for name, data in channel_mappings[user_id].items():
        pairs_list.append(
            f"📌 {name}\n"
            f"   ➡️ Route: {data['source']} → {data['destination']}\n"
            f"   ✅ Active: {'Yes' if data['active'] else 'No'}\n"
            f"   👤 Mentions: {'Removed' if data['remove_mentions'] else 'Kept'}\n"
            f"   🔗 URLs: {'Blocked' if data.get('block_urls', False) else 'Allowed'}\n"
            f"   🔗 URL BL: {len(data.get('blacklist_urls', []))}\n"
            f"   📑 Header: '{data.get('header_pattern', '') or 'None'}'\n"
            f"   📑 Footer: '{data.get('footer_pattern', '') or 'None'}'\n"
            f"   ✍️ Custom H: '{data.get('custom_header', '') or 'None'}'\n"
            f"   ✍️ Custom F: '{data.get('custom_footer', '') or 'None'}'\n"
            f"   🔍 Filters: BL: {len(data.get('blacklist', []))} | BS: {len(data.get('blocked_sentences', []))} | BI: {len(data.get('blocked_image_hashes', []))}\n"
            f"---------------"
        )
    full_message = header + "\n".join(pairs_list)
    await send_split_message_event(event, full_message)

@client.on(events.NewMessage(pattern='(?i)^/pausepair (\S+)$'))
async def pause_pair(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['active'] = False
        save_mappings()
        await event.reply(f"⏸️ Pair '{pair_name}' paused.")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/startpair (\S+)$'))
async def start_pair(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['active'] = True
        save_mappings()
        await event.reply(f"▶️ Pair '{pair_name}' activated.")
    else:
        await event.reply("❌ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearpairs$'))
async def clear_pairs(event):
    user_id = str(event.sender_id)
    if user_id in channel_mappings:
        channel_mappings[user_id] = {}
        pair_stats[user_id] = {}
        save_mappings()
        await event.reply("🗑️ All pairs cleared.")
    else:
        await event.reply("❌ No pairs to clear.")

@client.on(events.NewMessage)
async def forward_messages(event):
    """Queue incoming messages for forwarding with timestamp."""
    queued_time = datetime.now()
    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['active'] and event.chat_id == int(mapping['source']):
                message_queue.append((event, mapping, user_id, pair_name, queued_time))
                pair_stats[user_id][pair_name]['queued'] += 1
                logger.info(f"Message queued for '{pair_name}' at {queued_time.isoformat()}")
                return

@client.on(events.MessageEdited)
async def handle_message_edit(event):
    """Handle edits to source messages."""
    if not is_connected:
        return
    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['active'] and event.chat_id == int(mapping['source']):
                try:
                    await edit_forwarded_message(event, mapping, user_id, pair_name)
                except Exception as e:
                    logger.error(f"Error editing for '{pair_name}': {e}")
                return

@client.on(events.MessageDeleted)
async def handle_message_deleted(event):
    """Handle deletions of source messages."""
    if not is_connected:
        return
    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['active'] and event.chat_id == int(mapping['source']):
                try:
                    for deleted_id in event.deleted_ids:
                        event.message.id = deleted_id
                        await delete_forwarded_message(event, mapping, user_id, pair_name)
                except Exception as e:
                    logger.error(f"Error handling deletion for '{pair_name}': {e}")
                return

async def check_connection_status():
    """Monitor and update connection status."""
    global is_connected
    while True:
        current_status = client.is_connected()
        if current_status and not is_connected:
            is_connected = True
            logger.info("📡 Connection established")
        elif not current_status and is_connected:
            is_connected = False
            logger.warning("📡 Connection lost")
        await asyncio.sleep(5)

async def queue_processor():
    """Process queued messages with a delay."""
    while True:
        if is_connected and message_queue:
            message_data = message_queue.popleft()
            event, mapping, user_id, pair_name, _ = message_data  # Ignore queued_time here
            await forward_message_with_retry(event, mapping, user_id, pair_name)
            await asyncio.sleep(FORWARD_DELAY)
        else:
            await asyncio.sleep(1)

async def check_queue_inactivity():
    """Check for messages stuck in queue too long and notify."""
    while True:
        await asyncio.sleep(60)  # Check every minute
        if not is_connected or not NOTIFY_CHAT_ID or not message_queue:
            continue
        current_time = datetime.now()
        for i, (event, mapping, user_id, pair_name, queued_time) in enumerate(message_queue):
            wait_duration = (current_time - queued_time).total_seconds()
            if wait_duration > QUEUE_INACTIVITY_THRESHOLD:
                source_msg_id = event.message.id if hasattr(event.message, 'id') else "Unknown"
                alert_msg = (
                    f"⏳ Queue Inactivity Alert: Message for pair '{pair_name}' "
                    f"(Source Msg ID: {source_msg_id}) has been in queue for "
                    f"{int(wait_duration // 60)} minutes. Queue size: {len(message_queue)}"
                )
                logger.warning(alert_msg)
                await client.send_message(NOTIFY_CHAT_ID, alert_msg)
                # Optionally remove or mark as notified to avoid repeat alerts
                break  # Notify only the oldest message to avoid spam

async def check_pair_inactivity():
    """Notify about inactive pairs."""
    while True:
        await asyncio.sleep(300)
        if not is_connected or not NOTIFY_CHAT_ID:
            continue
        current_time = datetime.now()
        for user_id, pairs in channel_mappings.items():
            for pair_name, mapping in pairs.items():
                if not mapping['active']:
                    continue
                stats = pair_stats.get(user_id, {}).get(pair_name, {})
                last_activity_str = stats.get('last_activity')
                if not last_activity_str:
                    continue
                last_activity = datetime.fromisoformat(last_activity_str)
                inactivity_duration = (current_time - last_activity).total_seconds()
                if inactivity_duration > INACTIVITY_THRESHOLD:
                    await client.send_message(
                        NOTIFY_CHAT_ID,
                        f"⏰ Inactivity Alert: Pair '{pair_name}' inactive for over {INACTIVITY_THRESHOLD // 3600} hours."
                    )
                    pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()

async def send_periodic_report():
    """Send periodic stats report."""
    while True:
        await asyncio.sleep(21600)  # 6 hours
        if not is_connected or not MONITOR_CHAT_ID:
            continue
        for user_id in channel_mappings:
            header = "📊 6-Hour Report\n--------------------\n"
            report = []
            total_queued = len(message_queue)
            for pair_name, data in channel_mappings[user_id].items():
                stats = pair_stats.get(user_id, {}).get(pair_name, {
                    'forwarded': 0, 'edited': 0, 'deleted': 0, 'blocked': 0, 'queued': 0, 'last_activity': None
                })
                report.append(
                    f"📌 {pair_name}\n"
                    f"   ➡️ Route: {data['source']} → {data['destination']}\n"
                    f"   ✅ Status: {'Active' if data['active'] else 'Paused'}\n"
                    f"   📈 Fwd: {stats['forwarded']} | Edt: {stats['edited']} | Del: {stats['deleted']}\n"
                    f"   🚫 Blk: {stats['blocked']} | 📥 Que: {stats['queued']}\n"
                    f"---------------"
                )
            full_message = header + "\n".join(report) + f"\n📥 Queued: {total_queued}"
            try:
                await client.send_message(MONITOR_CHAT_ID, full_message)
                logger.info("Sent periodic report")
            except Exception as e:
                logger.error(f"Error sending report: {e}")

async def heartbeat():
    """Ensure bot stays connected."""
    while True:
        await asyncio.sleep(300)  # 5 minutes
        if not await client.is_user_authorized():
            logger.error("📡 Disconnected, attempting to reconnect...")
            await client.connect()

async def main():
    """Main bot initialization and runtime."""
    load_mappings()
    tasks = [
        check_connection_status(),
        send_periodic_report(),
        check_pair_inactivity(),
        heartbeat(),
        queue_processor(),
        check_queue_inactivity()  # New task for queue monitoring
    ]
    for task in tasks:
        asyncio.create_task(task)
    logger.info("🤖 Bot is starting...")

    try:
        await client.start()
        if not await client.is_user_authorized():
            phone = input("Please enter your phone (or bot token): ")
            await client.start(phone=phone)
            code = input("Please enter the verification code you received: ")
            await client.sign_in(phone=phone, code=code)

        global is_connected, MONITOR_CHAT_ID, NOTIFY_CHAT_ID
        is_connected = client.is_connected()
        MONITOR_CHAT_ID = (await client.get_me()).id
        NOTIFY_CHAT_ID = MONITOR_CHAT_ID  # Ensure NOTIFY_CHAT_ID is set

        if is_connected:
            logger.info("📡 Initial connection established")
        else:
            logger.warning("📡 Initial connection not established")

        await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
    finally:
        logger.info("🤖 Bot is shutting down...")
        save_mappings()

if __name__ == "__main__":
    try:
        client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("🤖 Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}", exc_info=True)
