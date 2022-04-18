# Проверка и привидение к формату номера телефона
import re

DEBUG = True
white_lst = [
    "79194927359",
    "79523395839",
    "79223736406"
]


def e164(phone):
    result = re.sub(r'\D', "", phone)

    try:
        int(result)
    except ValueError:
        return None

    if len(result) == 11:
        if result.startswith("8"):
            result = '7' + result[1:]
        if result.startswith("79") or result.startswith("7342"):
            if DEBUG:
                # return "79082419652"
                # return "79223307885"
                if result in white_lst:
                    print(f"WhiteListed number: {result}")
                    # return "79082419652"
                    # return "79223307885"
                    return result
                else:
                    return None
            else:
                return result
        else:
            return None
    else:
        return None
