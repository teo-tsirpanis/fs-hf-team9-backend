using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;

public record VoteRequest(bool VoteForQuit);

public record Response(int VotedYes, int VotedNo, float QuitThreshold);

public sealed class RoomState
{
    public const float QuitThreshold = 0.75f;

    private readonly Lock _lock = new();
    private readonly Dictionary<string, bool?> _members = new();

    private bool _ended;

    // Ended is true only on the single change that makes the yes ratio exceed the threshold.
    public (Response Response, bool Ended) Join(string connectionId)
    {
        lock (_lock)
        {
            _members.TryAdd(connectionId, null);
            return (Snapshot(), CheckEnded());
        }
    }

    public (Response Response, bool Empty, bool Ended) Leave(string connectionId)
    {
        lock (_lock)
        {
            _members.Remove(connectionId);
            return (Snapshot(), _members.Count == 0, CheckEnded());
        }
    }

    public (Response Response, bool Ended)? Vote(string connectionId, bool voteForQuit)
    {
        lock (_lock)
        {
            if (!_members.ContainsKey(connectionId)) return null;
            _members[connectionId] = voteForQuit;
            return (Snapshot(), CheckEnded());
        }
    }

    private bool CheckEnded()
    {
        if (_ended || _members.Count == 0) return false;
        var yes = _members.Values.Count(v => v == true);
        if ((float)yes / _members.Count <= QuitThreshold) return false;
        return _ended = true;
    }

    private Response Snapshot() =>
        new(_members.Values.Count(v => v == true), _members.Values.Count(v => v == false), QuitThreshold);

    public bool IsEmpty { get { lock (_lock) return _members.Count == 0; } }
}

public sealed class RoomRegistry
{
    private readonly ConcurrentDictionary<string, RoomState> _rooms = new();

    public RoomState GetOrAdd(string callId) => _rooms.GetOrAdd(callId, _ => new RoomState());

    public bool TryGet(string callId, out RoomState room) => _rooms.TryGetValue(callId, out room!);

    public void RemoveIfEmpty(string callId, RoomState room)
    {
        if (room.IsEmpty) _rooms.TryRemove(new KeyValuePair<string, RoomState>(callId, room));
    }
}

public sealed class VoteHub(RoomRegistry rooms) : Hub
{
    private const string CallsKey = "calls";

    private HashSet<string> JoinedCalls =>
        (HashSet<string>)(Context.Items[CallsKey] ??= new HashSet<string>());

    public async Task JoinCall(string callId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(callId);
        await Groups.AddToGroupAsync(Context.ConnectionId, callId);
        JoinedCalls.Add(callId);
        var (response, ended) = rooms.GetOrAdd(callId).Join(Context.ConnectionId);
        await Clients.Group(callId).SendAsync("VotesUpdated", response);
        if (ended) await Clients.Group(callId).SendAsync("MeetingEnded", response);
    }

    public Task LeaveCall(string callId) => RemoveFromCall(callId);

    public async Task Vote(string callId, VoteRequest request)
    {
        if (!rooms.TryGet(callId, out var room))
            throw new HubException("Join the call before voting.");
        var (response, ended) = room.Vote(Context.ConnectionId, request.VoteForQuit)
            ?? throw new HubException("Join the call before voting.");
        await Clients.Group(callId).SendAsync("VotesUpdated", response);
        if (ended) await Clients.Group(callId).SendAsync("MeetingEnded", response);
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        foreach (var callId in JoinedCalls.ToArray())
            await RemoveFromCall(callId);
        await base.OnDisconnectedAsync(exception);
    }

    private async Task RemoveFromCall(string callId)
    {
        JoinedCalls.Remove(callId);
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, callId);
        if (!rooms.TryGet(callId, out var room)) return;
        var (response, empty, ended) = room.Leave(Context.ConnectionId);
        if (empty) { rooms.RemoveIfEmpty(callId, room); return; }
        await Clients.Group(callId).SendAsync("VotesUpdated", response);
        if (ended) await Clients.Group(callId).SendAsync("MeetingEnded", response);
    }
}
