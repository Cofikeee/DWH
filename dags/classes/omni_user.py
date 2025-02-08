def get_user_roles_array(user):
    roles = {
        'cf_7012': 'Администратор',
        'cf_7011': 'Кадровик',
        'cf_7010': 'Руководитель',
        'cf_7013': 'Делопроизводитель',
    }
    roles_array = []
    for cf_key, value in roles.items():
        if user.get('custom_fields', {}).get(cf_key):
            roles_array += [value]
    return roles_array  # if len(roles_array) > 0 else None


def fix_channel(response):
    channel_mapping = {
        'phone': 'user_phone',
        'telegram': 'telegram_id',
        'email': 'user_email',
        'vkontakte': 'vkontakte_id',
        'facebook': 'facebook_id',
        'wa': 'wa_id',
        'mattermost': 'mattermost_id'
    }
    return channel_mapping.get(response, response)

