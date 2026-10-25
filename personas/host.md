---
# Machine half of the persona. tv-recommender pack_typesafe sends these
# statements to the typesafe classifier (laya) one show at a time; each comes
# back as P(yes). The verdict is swamp's: TMDB genre lanes, RT quality bar,
# and these answers by role (see `evaluateShow` in @keeb/mms tv_recommender).
# Keep each statement short and about one thing — the classifier reads the
# show's overview, not this doc.
rubric:
  threshold: 0.5
  # Any one at/over threshold puts a show in lane even with no lane genre.
  hook:
    sleuth: The main character is a detective, agent or investigator.
    hunt: Someone hunts a killer.
    thriller: This is a thriller.
    supernatural: Something supernatural happens.
  # Skip when one of these is at/over threshold and beats every hook.
  offLane:
    comedy: This show is mainly a comedy.
    romance: This show is mainly a love story or romance.
  # Against a show with a lane genre, an off-lane statement only counts when
  # TMDB tags that genre too. Laya calls dark dramas (Baby Reindeer, The Deuce,
  # House) "comedy"; TMDB doesn't. Romance has no TMDB TV genre, so it stands alone.
  offLaneGenres:
    comedy: Comedy
  # Mei's two limits. At/over vetoThreshold is always Skip. Laya reads story-
  # driven violence (Silo, The Night Of, Spartacus) as gore in the 0.5-0.78
  # range, so the hard line demands more certainty than the soft roles.
  vetoThreshold: 0.8
  veto:
    rape: The premise is the rape or sexual enslavement of women.
    gore: The show is gore and torture with no real story.
---

# The Host

Would our group stream this show? We love dark, acclaimed drama in three
lanes: crime and serial-killer hunts with morally grey leads
(Hannibal, Furious, True Detective); slow-burn horror or mystery in a small
town or sealed place (FROM, Hill House, Mare of Easttown); high-concept sci-fi
(Severance, Westworld). Violence and bleakness are fine. Skip pure comedy,
romance, family, period and teen drama, and any show built on rape of women
or gore for its own sake.

## What we watch

I host. I stream TV to my friends over Discord; it's where the memes are
born. Four lanes pull us in:

- **Prestige crime drama.** A killer, a crime family or a corrupt system,
  seen from both sides, with a lead who is compromised or worse. The Wire,
  Sopranos, Hannibal, True Detective, Fargo, Barry, Dexter, Furious, and the
  procedural end of it: Mentalist, Sherlock, Reacher.
- **Atmospheric horror / mystery.** A death or a disappearance opens a
  mystery in a place you can't leave, with dread over jump scares and often
  something supernatural underneath. Hill House, Usher, Sharp Objects, The
  Outsider, Mare of Easttown, IT: Welcome to Derry, FROM, Paradise,
  Yellowjackets, Widow's Bay.
- **High-concept / weird.** The premise carries the show; a slow start is
  fine if the concept earns it. Severance, Westworld, 3 Body Problem,
  Invasion, The Rehearsal.
- **Mainstream event.** Cultural moments worth being current on. Squid Game,
  Stranger Things, Game of Thrones.

Movies follow the same lanes and only happen on group movie night: Heretic,
Nosferatu, Weapons, American Psycho, Rebel Ridge, Anora, Babygirl.

## What those shows have in common (from TMDB)

- **Genre.** Nearly every one is tagged Drama, plus Crime, Mystery or
  Sci-Fi & Fantasy. Crime + Drama + Mystery is the single most common set.
  Comedy only appears blended into a drama or crime show (Barry, Widow's
  Bay, The Rehearsal); we have never taken up a pure comedy.
- **The hook is a killing or a vanishing.** Most overviews open on a
  murder, a serial killer, or people who disappear (Mare, Sharp Objects,
  The Outsider, Paradise, Hannibal, Dexter, Furious, IT, Stranger Things,
  Weapons).
- **A sealed place.** A small town (Easttown, Derry, Hawkins, Widow's Bay,
  Fargo's Minnesota), a house (Hill House, Usher), or a system you can't
  leave (FROM's town, Severance's office, Westworld's park, Squid Game's
  arena, Paradise's bunker city).
- **An old evil under the surface.** A curse, a monster, a family rot, an
  ancient thing waking up (The Outsider, IT, FROM, Game of Thrones, Usher,
  Widow's Bay, Nosferatu).
- **Good and evil blur.** The overviews say it outright: "distinctions
  between good and evil are routinely obliterated" (The Wire), "the line
  between right and wrong begins to blur" (Furious).
- **Quality.** TMDB ratings around 7.5–8.7, mostly HBO, Apple TV, Netflix,
  Hulu, FX and MGM+.

What never shows up: sitcoms, romance-led stories, family viewing,
coming-of-age, costume or period romance, and sports drama.

## The group

- **Suree** (my wife) watches everything with us.
- **Mei** is my main co-watcher for the horror and mystery lane, and asks
  for scary movie nights.
- **Zachk** likes prestige procedurals and action: The Wire, Mentalist,
  Sopranos.

Mei has exactly two limits, and both are narrow. Don't turn them into
"too dark": graphic violence, serial killers, body horror and bleak
endings all land with her (Hannibal, Game of Thrones, FROM, Furious).

1. **Rape of women as the premise**, as in The Handmaid's Tale. A sexual
   assault that happens in a story (as in Game of Thrones) is not the same
   thing.
2. **Dismemberment for its own sake**, as in Terrifier 3: gore is the whole
   point and there's no story carrying it.

## How I score

- **Watch:** solidly in a lane. It's a drama with the hooks above, and well
  reviewed.
- **Skip:** not solidly in a lane, or it trips one of Mei's two limits.
  The frontmatter rubric is how the automated pass applies both: a classifier
  answers the statements, and swamp combines them with the genre lanes and
  RT scores.
- **Auto-include:** a new season of something we already watch (True
  Detective, Yellowjackets, Love Is Blind). Reality is only a group event
  when it's a show we already love, like Love Is Blind.
