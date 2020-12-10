use Mojolicious::Lite -signatures;
use Mojo::Pg;

helper pg => sub { state $pg = Mojo::Pg->new('postgresql://postgres@/test') };

get '/' => 'chat';

websocket '/channel' => sub ($c) {
  $c->inactivity_timeout(3600);

  # Forward messages from the browser to PostgreSQL
  $c->on(message => sub ($c, $message) { $c->pg->pubsub->notify(mojochat => $message) });

  # Forward messages from PostgreSQL to the browser
  my $cb = $c->pg->pubsub->listen(mojochat => sub ($pubsub, $message) { $c->send($message) });
  $c->on(finish => sub ($c) { $c->pg->pubsub->unlisten(mojochat => $cb) });
};

app->start;
__DATA__

@@ chat.html.ep
<form onsubmit="sendChat(this.children[0]); return false"><input></form>
<div id="log"></div>
<script>
  var ws  = new WebSocket('<%= url_for('channel')->to_abs %>');
  ws.onmessage = function (e) {
    document.getElementById('log').innerHTML += '<p>' + e.data + '</p>';
  };
  function sendChat(input) { ws.send(input.value); input.value = '' }
</script>
