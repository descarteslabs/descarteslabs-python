import requests
from descarteslabs.client.services.tasks import Tasks, FutureTask


def receive(*args, **kwargs):
    import requests
    import json

    slack_payload = dict(
        text=json.dumps(dict(args=args, kwargs=kwargs)),
        channel="#slack-testing"
    )

    r = requests.post("https://hooks.slack.com/services/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",
                      json=slack_payload)

    return r.content


at = Tasks()

# create or reuse existing group
group_id = at.new_group(receive, name="webhook", memory='0.25Gi')['id']
print(group_id)

webhook = at.create_webhook(group_id)
print(webhook['url'])

# test it out
r = requests.post(webhook['url'], json=dict(msg="hello webhook", foo="bar")).json()

# task created by webhook
print(r)

result = FutureTask(guid=group_id, tuid=r['id'], client=at).get_result(wait=True, timeout=300)
print(result)
